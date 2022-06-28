////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#if REALM_ENABLE_AUTH_TESTS

#include <catch2/catch_all.hpp>

#include "flx_sync_harness.hpp"
#include "sync/sync_test_utils.hpp"
#include "util/test_file.hpp"
#include "realm/object-store/impl/object_accessor_impl.hpp"
#include "realm/object-store/impl/realm_coordinator.hpp"
#include "realm/object-store/schema.hpp"
#include "realm/object-store/sync/generic_network_transport.hpp"
#include "realm/object-store/sync/sync_session.hpp"
#include "realm/object_id.hpp"
#include "realm/query_expression.hpp"
#include "realm/sync/client_base.hpp"
#include "realm/sync/config.hpp"
#include "realm/sync/noinst/client_history_impl.hpp"
#include "realm/sync/noinst/pending_bootstrap_store.hpp"
#include "realm/sync/protocol.hpp"
#include "realm/sync/subscriptions.hpp"
#include "realm/util/future.hpp"
#include "realm/util/logger.hpp"
#include "util/test_file.hpp"
#include <realm/sync/noinst/server/access_token.hpp>

#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace realm {

class TestHelper {
public:
    static bool can_advance(const SharedRealm& realm)
    {
        auto& coord = Realm::Internal::get_coordinator(*realm);
        return coord.can_advance(*realm);
    }
};

} // namespace realm

namespace realm::app {

namespace {
const Schema g_minimal_schema{
    {"TopLevel",
     {
         {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
     }},
};

const Schema g_large_array_schema{
    ObjectSchema("TopLevel",
                 {
                     {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                     {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
                     {"list_of_strings", PropertyType::Array | PropertyType::String},
                 }),
};

const Schema g_simple_embedded_obj_schema{
    {"TopLevel",
     {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
      {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
      {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "TopLevel_embedded_obj"}}},
    {"TopLevel_embedded_obj",
     ObjectSchema::ObjectType::Embedded,
     {
         {"str_field", PropertyType::String | PropertyType::Nullable},
     }},
};

// Populates a FLXSyncTestHarness with the g_large_array_schema with objects that are large enough that
// they are guaranteed to fill multiple bootstrap download messages. Currently this means generating 5
// objects each with 1024 array entries of 1024 bytes each.
//
// Returns a list of the _id values for the objects created.
std::vector<ObjectId> fill_large_array_schema(FLXSyncTestHarness& harness)
{
    std::vector<ObjectId> ret;
    REQUIRE(harness.schema() == g_large_array_schema);
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        for (int i = 0; i < 5; ++i) {
            auto id = ObjectId::gen();
            auto obj = Object::create(c, realm, "TopLevel",
                                      util::Any(AnyDict{{"_id", id},
                                                        {"list_of_strings", AnyVector{}},
                                                        {"queryable_int_field", static_cast<int64_t>(i * 5)}}));
            List str_list(obj, realm->schema().find("TopLevel")->property_for_name("list_of_strings"));
            for (int j = 0; j < 1024; ++j) {
                str_list.add(c, util::Any(std::string(1024, 'a' + (j % 26))));
            }

            ret.push_back(id);
        }
    });
    return ret;
}

void wait_for_advance(const SharedRealm& realm)
{
    timed_wait_for([&] {
        return !TestHelper::can_advance(realm);
    });
}
} // namespace

TEST_CASE("flx: connect to FLX-enabled app", "[sync][flx][app]") {
    FLXSyncTestHarness harness("basic_flx_connect");

    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", foo_obj_id},
                                         {"queryable_str_field", std::string{"foo"}},
                                         {"queryable_int_field", static_cast<int64_t>(5)},
                                         {"non_queryable_field", std::string{"non queryable 1"}}}));
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", bar_obj_id},
                                         {"queryable_str_field", std::string{"bar"}},
                                         {"queryable_int_field", static_cast<int64_t>(10)},
                                         {"non_queryable_field", std::string{"non queryable 2"}}}));
    });


    harness.do_with_new_realm([&](SharedRealm realm) {
        wait_for_download(*realm);
        {
            auto empty_subs = realm->get_latest_subscription_set();
            CHECK(empty_subs.size() == 0);
            CHECK(empty_subs.version() == 0);
            empty_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        auto table = realm->read_group().get_table("class_TopLevel");
        auto col_key = table->get_column_key("queryable_str_field");
        Query query_foo(table);
        query_foo.equal(col_key, "foo");
        {
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(query_foo);
            auto subs = std::move(new_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        wait_for_download(*realm);
        {
            wait_for_advance(realm);
            Results results(realm, table);
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == foo_obj_id);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            Query new_query_bar(table);
            new_query_bar.equal(col_key, "bar");
            mut_subs.insert_or_assign(new_query_bar);
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(realm);
            Results results(realm, Query(table));
            CHECK(results.size() == 2);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            auto it = mut_subs.find(query_foo);
            CHECK(it != mut_subs.end());
            mut_subs.erase(it);
            Query new_query_bar(table);
            new_query_bar.equal(col_key, "bar");
            mut_subs.insert_or_assign(new_query_bar);
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(realm);
            Results results(realm, Query(table));
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == bar_obj_id);
        }

        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            auto subs = std::move(mut_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(realm);
            Results results(realm, table);
            CHECK(results.size() == 0);
        }
    });
}

static auto make_error_handler()
{
    auto [error_promise, error_future] = util::make_promise_future<SyncError>();
    auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
    auto fn = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>, SyncError err) {
        error_promise->emplace_value(std::move(err));
    };
    return std::make_pair(std::move(error_future), std::move(fn));
}

static auto make_client_reset_handler()
{
    auto [reset_promise, reset_future] = util::make_promise_future<ClientResyncMode>();
    auto shared_promise = std::make_shared<decltype(reset_promise)>(std::move(reset_promise));
    auto fn = [reset_promise = std::move(shared_promise)](SharedRealm, ThreadSafeReference, bool did_recover) {
        reset_promise->emplace_value(did_recover ? ClientResyncMode::Recover : ClientResyncMode::DiscardLocal);
    };
    return std::make_pair(std::move(reset_future), std::move(fn));
}

TEST_CASE("flx: client reset", "[sync][flx][app][client reset]") {
    Schema schema{
        {"TopLevel",
         {
             {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
             {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
             {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
             {"non_queryable_field", PropertyType::String | PropertyType::Nullable},
             {"list_of_ints_field", PropertyType::Int | PropertyType::Array},
             {"sum_of_list_field", PropertyType::Int},
         }},
    };

    // some of these tests make additive schema changes which is only allowed in dev mode
    constexpr bool dev_mode = true;
    FLXSyncTestHarness harness("flx_client_reset",
                               {schema, {"queryable_str_field", "queryable_int_field"}, {}, dev_mode});

    auto add_object = [](SharedRealm realm, std::string str_field, int64_t int_field,
                         ObjectId oid = ObjectId::gen()) {
        CppContext c(realm);
        realm->begin_transaction();
        int64_t r1 = random_int();
        int64_t r2 = random_int();
        int64_t r3 = random_int();

        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", oid},
                                         {"queryable_str_field", str_field},
                                         {"queryable_int_field", int_field},
                                         {"non_queryable_field", std::string{"non queryable 1"}},
                                         {"list_of_ints_field", std::vector<util::Any>{r1, r2, r3}},
                                         {"sum_of_list_field", r1 + r2 + r3}}));
        realm->commit_transaction();
    };

    auto subscribe_to_and_add_objects = [&](SharedRealm realm, size_t num_objects) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto id_col = table->get_primary_key_column();
        auto sub_set = realm->get_latest_subscription_set();
        for (size_t i = 0; i < num_objects; ++i) {
            auto oid = ObjectId::gen();
            auto mut_sub = sub_set.make_mutable_copy();
            mut_sub.clear();
            mut_sub.insert_or_assign(Query(table).equal(id_col, oid));
            sub_set = std::move(mut_sub).commit();
            add_object(realm, util::format("added _id='%1'", oid), 0, oid);
        }
    };

    auto add_subscription_for_new_object = [&](SharedRealm realm, std::string str_field,
                                               int64_t int_field) -> sync::SubscriptionSet {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto sub_set = realm->get_latest_subscription_set().make_mutable_copy();
        sub_set.insert_or_assign(Query(table).equal(queryable_str_field, StringData(str_field)));
        auto resulting_set = std::move(sub_set).commit();
        add_object(realm, str_field, int_field);
        return resulting_set;
    };

    auto add_invalid_subscription = [&](SharedRealm realm) -> sync::SubscriptionSet {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("non_queryable_field");
        auto sub_set = realm->get_latest_subscription_set().make_mutable_copy();
        sub_set.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
        auto resulting_set = std::move(sub_set).commit();
        return resulting_set;
    };

    auto count_queries_with_str = [](sync::SubscriptionSet subs, std::string_view str) {
        size_t count = 0;
        for (auto sub : subs) {
            if (sub.query_string().find(str) != std::string::npos) {
                ++count;
            }
        }
        return count;
    };
    create_user_and_log_in(harness.app());
    auto user1 = harness.app()->current_user();
    create_user_and_log_in(harness.app());
    auto user2 = harness.app()->current_user();
    SyncTestFile config_local(user1, harness.schema(), SyncConfig::FLXSyncEnabled{});
    config_local.path += ".local";
    SyncTestFile config_remote(user2, harness.schema(), SyncConfig::FLXSyncEnabled{});
    config_remote.path += ".remote";
    const std::string str_field_value = "foo";
    const int64_t local_added_int = 100;
    const int64_t remote_added_int = 200;
    size_t before_reset_count = 0;
    size_t after_reset_count = 0;
    config_local.sync_config->notify_before_client_reset = [&before_reset_count](SharedRealm) {
        ++before_reset_count;
    };
    config_local.sync_config->notify_after_client_reset = [&after_reset_count](SharedRealm, ThreadSafeReference,
                                                                               bool) {
        ++after_reset_count;
    };

    SECTION("Recover: offline writes and subscriptions") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [reset_future, reset_handler] = make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::Recover);
                auto subs = local_realm->get_latest_subscription_set();
                subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                // make sure that the subscription for "foo" survived the reset
                size_t count_of_foo = count_queries_with_str(subs, util::format("\"%1\"", str_field_value));
                REQUIRE(subs.state() == sync::SubscriptionSet::State::Complete);
                REQUIRE(count_of_foo == 1);
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto str_col = table->get_column_key("queryable_str_field");
                auto int_col = table->get_column_key("queryable_int_field");
                auto tv = table->where().equal(str_col, StringData(str_field_value)).find_all();
                tv.sort(int_col);
                // the object we created while offline was recovered, and the remote object was downloaded
                REQUIRE(tv.size() == 2);
                CHECK(tv.get_object(0).get<Int>(int_col) == local_added_int);
                CHECK(tv.get_object(1).get<Int>(int_col) == remote_added_int);
            })
            ->run();
    }

    auto validate_integrity_of_arrays = [](TableRef table) -> size_t {
        auto sum_col = table->get_column_key("sum_of_list_field");
        auto array_col = table->get_column_key("list_of_ints_field");
        auto query = table->column<Lst<Int>>(array_col).sum() == table->column<Int>(sum_col) &&
                     table->column<Lst<Int>>(array_col).size() > 0;
        return query.count();
    };

    SECTION("Recover: offline writes with associated subscriptions in the correct order") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [reset_future, reset_handler] = make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        constexpr size_t num_objects_added = 20;
        constexpr size_t num_objects_added_by_harness = 1; // BaasFLXClientReset.run()
        constexpr size_t num_objects_added_by_remote = 1;  // make_remote_changes()
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                subscribe_to_and_add_objects(local_realm, num_objects_added);
                auto table = local_realm->read_group().get_table("class_TopLevel");
                REQUIRE(table->size() == num_objects_added + num_objects_added_by_harness);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::Recover);
                local_realm->refresh();
                auto latest_subs = local_realm->get_latest_subscription_set();
                auto state = latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                REQUIRE(state == sync::SubscriptionSet::State::Complete);
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                if (table->size() != 1) {
                    table->to_json(std::cout, 1, {});
                }
                REQUIRE(table->size() == 1);
                auto mut_sub = latest_subs.make_mutable_copy();
                mut_sub.clear();
                mut_sub.insert_or_assign(Query(table));
                latest_subs = std::move(mut_sub).commit();
                latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                local_realm->refresh();
                REQUIRE(table->size() ==
                        num_objects_added + num_objects_added_by_harness + num_objects_added_by_remote);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added + num_objects_added_by_remote);
            })
            ->run();
    }

    SECTION("Recover: incompatible property changes are rejected") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        constexpr size_t num_objects_added_before = 2;
        constexpr size_t num_objects_added_after = 2;
        constexpr size_t num_objects_added_by_harness = 1; // BaasFLXClientReset.run()
        constexpr std::string_view added_property_name = "new_property";
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                subscribe_to_and_add_objects(local_realm, num_objects_added_before);
                Schema local_update = schema;
                Schema::iterator it = local_update.find("TopLevel");
                REQUIRE(it != local_update.end());
                it->persisted_properties.push_back(
                    {std::string(added_property_name), PropertyType::Float | PropertyType::Nullable});
                local_realm->update_schema(local_update);
                subscribe_to_and_add_objects(local_realm, num_objects_added_after);
                auto table = local_realm->read_group().get_table("class_TopLevel");
                REQUIRE(table->size() ==
                        num_objects_added_before + num_objects_added_after + num_objects_added_by_harness);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added_before + num_objects_added_after);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
                Schema remote_update = schema;
                Schema::iterator it = remote_update.find("TopLevel");
                REQUIRE(it != remote_update.end());
                it->persisted_properties.push_back(
                    {std::string(added_property_name), PropertyType::UUID | PropertyType::Nullable});
                remote_realm->update_schema(remote_update);
                sync::SubscriptionSet::State actual =
                    remote_realm->get_latest_subscription_set()
                        .get_state_change_notification(sync::SubscriptionSet::State::Complete)
                        .get();
                REQUIRE(actual == sync::SubscriptionSet::State::Complete);
            })
            ->on_post_reset([&, err_future = std::move(error_future)](SharedRealm local_realm) {
                auto sync_error = std::move(err_future).get();
                REQUIRE(before_reset_count == 1);
                REQUIRE(after_reset_count == 0);
                REQUIRE(sync_error.error_code == sync::make_error_code(sync::ClientError::auto_client_reset_failure));
                REQUIRE(sync_error.is_client_reset_requested());
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                // since schema validation happens in the first recovery commit, that whole commit is rolled back
                // and the final state here is "pre reset"
                REQUIRE(table->size() ==
                        num_objects_added_before + num_objects_added_by_harness + num_objects_added_after);
                size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
                REQUIRE(count_of_valid_array_data == num_objects_added_before + num_objects_added_after);
            })
            ->run();
    }

    SECTION("unsuccessful replay of local changes") {
        constexpr size_t num_objects_added_before = 2;
        constexpr size_t num_objects_added_after = 2;
        constexpr size_t num_objects_added_by_harness = 1; // BaasFLXClientReset.run()
        constexpr std::string_view added_property_name = "new_property";
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;

        // The local changes here are a bit contrived because removing a column is disallowed
        // at the object store layer for sync'd Realms. The only reason a recovery should fail in production
        // during the apply stage is due to programmer error or external factors such as out of disk space.
        // Any schema discrepencies are caught by the initial diff, so the way to make a recovery fail here is
        // to add and remove a column at the core level such that the schema diff passes, but instructions are
        // generated which will fail when applied.
        reset_utils::TestClientReset::Callback make_local_changes_that_will_fail = [&](SharedRealm local_realm) {
            subscribe_to_and_add_objects(local_realm, num_objects_added_before);
            auto table = local_realm->read_group().get_table("class_TopLevel");
            REQUIRE(table->size() == num_objects_added_before + num_objects_added_by_harness);
            size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
            REQUIRE(count_of_valid_array_data == num_objects_added_before);
            local_realm->begin_transaction();
            ColKey added = table->add_column(type_Int, added_property_name);
            table->remove_column(added);
            local_realm->commit_transaction();
            subscribe_to_and_add_objects(local_realm, num_objects_added_after); // these are lost!
        };

        reset_utils::TestClientReset::Callback verify_post_reset_state = [&, err_future = std::move(error_future)](
                                                                             SharedRealm local_realm) {
            auto sync_error = std::move(err_future).get();
            REQUIRE(before_reset_count == 1);
            REQUIRE(after_reset_count == 0);
            REQUIRE(sync_error.error_code == sync::make_error_code(sync::ClientError::auto_client_reset_failure));
            REQUIRE(sync_error.is_client_reset_requested());
            local_realm->refresh();
            auto table = local_realm->read_group().get_table("class_TopLevel");
            ColKey added = table->get_column_key(added_property_name);
            REQUIRE(!added); // partial recovery halted at remove_column() but rolled back everything in the change
            // table is missing num_objects_added_after and the last commit after the latest subscription
            // this is due to how recovery batches together changesets up until a subscription
            constexpr size_t expected_added_objects = num_objects_added_before - 1;
            REQUIRE(table->size() == expected_added_objects + num_objects_added_by_harness);
            size_t count_of_valid_array_data = validate_integrity_of_arrays(table);
            REQUIRE(count_of_valid_array_data == expected_added_objects);
        };

        SECTION("Recover: unsuccessful recovery leads to a manual reset") {
            config_local.sync_config->client_resync_mode = ClientResyncMode::Recover;
            auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
            test_reset->make_local_changes(std::move(make_local_changes_that_will_fail))
                ->on_post_reset(std::move(verify_post_reset_state))
                ->run();
            auto config_copy = config_local;
            auto&& [error_future2, err_handler2] = make_error_handler();
            config_copy.sync_config->error_handler = err_handler2;
            auto realm_post_reset = Realm::get_shared_realm(config_copy);
            auto sync_error = std::move(error_future2).get();
            REQUIRE(before_reset_count == 2);
            REQUIRE(after_reset_count == 0);
            REQUIRE(sync_error.error_code == sync::make_error_code(sync::ClientError::auto_client_reset_failure));
            REQUIRE(sync_error.is_client_reset_requested());
        }

        SECTION("RecoverOrDiscard: unsuccessful reapply leads to discard") {
            config_local.sync_config->client_resync_mode = ClientResyncMode::RecoverOrDiscard;
            auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
            test_reset->make_local_changes(std::move(make_local_changes_that_will_fail))
                ->on_post_reset(std::move(verify_post_reset_state))
                ->run();

            auto config_copy = config_local;
            auto&& [client_reset_future, reset_handler] = make_client_reset_handler();
            config_copy.sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError err) {
                REALM_ASSERT_EX(!err.is_fatal, err.message);
            };
            config_copy.sync_config->notify_after_client_reset = reset_handler;
            auto realm_post_reset = Realm::get_shared_realm(config_copy);
            ClientResyncMode mode = client_reset_future.get();
            REQUIRE(mode == ClientResyncMode::DiscardLocal);
            realm_post_reset->refresh();
            auto table = realm_post_reset->read_group().get_table("class_TopLevel");
            ColKey added = table->get_column_key(added_property_name);
            REQUIRE(!added);                                        // reverted local changes
            REQUIRE(table->size() == num_objects_added_by_harness); // discarded all offline local changes
        }
    }

    SECTION("DiscardLocal: offline writes and subscriptions are lost") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [reset_future, reset_handler] = make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::DiscardLocal);
                auto subs = local_realm->get_latest_subscription_set();
                subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                local_realm->refresh();
                auto table = local_realm->read_group().get_table("class_TopLevel");
                auto queryable_str_field = table->get_column_key("queryable_str_field");
                auto queryable_int_field = table->get_column_key("queryable_int_field");
                auto tv = table->where().equal(queryable_str_field, StringData(str_field_value)).find_all();
                // the object we created while offline was discarded, and the remote object was not downloaded
                REQUIRE(tv.size() == 0);
                size_t count_of_foo = count_queries_with_str(subs, util::format("\"%1\"", str_field_value));
                // make sure that the subscription for "foo" did not survive the reset
                REQUIRE(count_of_foo == 0);
                REQUIRE(subs.state() == sync::SubscriptionSet::State::Complete);

                // adding data and subscriptions to a reset Realm works as normal
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
                auto latest_subs = local_realm->get_latest_subscription_set();
                REQUIRE(latest_subs.version() > subs.version());
                latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                local_realm->refresh();
                count_of_foo = count_queries_with_str(latest_subs, util::format("\"%1\"", str_field_value));
                REQUIRE(count_of_foo == 1);
                tv = table->where().equal(queryable_str_field, StringData(str_field_value)).find_all();
                REQUIRE(tv.size() == 2);
                tv.sort(queryable_int_field);
                REQUIRE(tv.get_object(0).get<int64_t>(queryable_int_field) == local_added_int);
                REQUIRE(tv.get_object(1).get<int64_t>(queryable_int_field) == remote_added_int);
            })
            ->run();
    }

    SECTION("DiscardLocal: an invalid subscription made while offline becomes superceeded") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [reset_future, reset_handler] = make_client_reset_handler();
        config_local.sync_config->notify_after_client_reset = reset_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        std::unique_ptr<sync::SubscriptionSet> invalid_sub;
        test_reset
            ->make_local_changes([&](SharedRealm local_realm) {
                invalid_sub = std::make_unique<sync::SubscriptionSet>(add_invalid_subscription(local_realm));
                add_subscription_for_new_object(local_realm, str_field_value, local_added_int);
            })
            ->make_remote_changes([&](SharedRealm remote_realm) {
                add_subscription_for_new_object(remote_realm, str_field_value, remote_added_int);
            })
            ->on_post_reset([&, client_reset_future = std::move(reset_future)](SharedRealm local_realm) {
                local_realm->refresh();
                sync::SubscriptionSet::State actual =
                    invalid_sub->get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                REQUIRE(actual == sync::SubscriptionSet::State::Superseded);
                ClientResyncMode mode = client_reset_future.get();
                REQUIRE(mode == ClientResyncMode::DiscardLocal);
            })
            ->run();
    }

    SECTION("DiscardLocal: an error is produced if a previously successful query becomes invalid due to "
            "server changes across a reset") {
        config_local.sync_config->client_resync_mode = ClientResyncMode::DiscardLocal;
        auto&& [error_future, err_handler] = make_error_handler();
        config_local.sync_config->error_handler = err_handler;
        auto test_reset = reset_utils::make_baas_flx_client_reset(config_local, config_remote, harness.session());
        test_reset
            ->setup([&](SharedRealm realm) {
                if (realm->sync_session()->path() == config_local.path) {
                    auto added_sub = add_subscription_for_new_object(realm, str_field_value, 0);
                    added_sub.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
                }
            })
            ->make_local_changes([&](SharedRealm local_realm) {
                add_object(local_realm, str_field_value, local_added_int);
                // Make "queryable_str_field" not a valid query field.
                // Pre-reset, the Realm had a successful query on it, but now when the client comes back online
                // and tries to reset, the fresh Realm download will fail with a query error.
                const AppSession& app_session = harness.session().app_session();
                auto baas_sync_service = app_session.admin_api.get_sync_service(app_session.server_app_id);
                auto baas_sync_config =
                    app_session.admin_api.get_config(app_session.server_app_id, baas_sync_service);
                REQUIRE(baas_sync_config.queryable_field_names->is_array());
                auto it = baas_sync_config.queryable_field_names->begin();
                for (; it != baas_sync_config.queryable_field_names->end(); ++it) {
                    if (*it == "queryable_str_field") {
                        break;
                    }
                }
                REQUIRE(it != baas_sync_config.queryable_field_names->end());
                baas_sync_config.queryable_field_names->erase(it);
                app_session.admin_api.enable_sync(app_session.server_app_id, baas_sync_service.id, baas_sync_config);
            })
            ->on_post_reset([&, err_future = std::move(error_future)](SharedRealm) {
                auto sync_error = std::move(err_future).get();
                // There is a race here depending on if the server produces a query error or responds to
                // the ident message first. We consider either error to be a sufficient outcome.
                if (sync_error.error_code == sync::make_error_code(sync::ClientError::auto_client_reset_failure)) {
                    CHECK(sync_error.is_client_reset_requested());
                }
                else {
                    CHECK(sync_error.error_code == sync::make_error_code(sync::ProtocolError::bad_query));
                }
            })
            ->run();
    }
}

TEST_CASE("flx: creating an object on a class with no subscription throws", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bad_query", {g_simple_embedded_obj_schema, {"queryable_str_field"}});
    harness.do_with_new_user([&](auto user) {
        SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
        auto [error_promise, error_future] = util::make_promise_future<SyncError>();
        auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
        config.sync_config->error_handler = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>,
                                                                                        SyncError err) {
            error_promise->emplace_value(std::move(err));
        };

        REQUIRE_THROWS_AS(
            [&] {
                auto realm = Realm::get_shared_realm(config);
                CppContext c(realm);
                realm->begin_transaction();
                Object::create(
                    c, realm, "TopLevel",
                    util::Any(AnyDict{{"_id", ObjectId::gen()}, {"queryable_str_field", std::string{"foo"}}}));
                realm->commit_transaction();
            }(),
            NoSubscriptionForWrite);

        auto realm = Realm::get_shared_realm(config);
        auto table = realm->read_group().get_table("class_TopLevel");

        REQUIRE(table->is_empty());
        auto col_key = table->get_column_key("queryable_str_field");
        {
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(Query(table).equal(col_key, "foo"));
            auto subs = std::move(new_subs).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        CppContext c(realm);
        realm->begin_transaction();
        auto obj = Object::create(c, realm, "TopLevel",
                                  util::Any(AnyDict{{"_id", ObjectId::gen()},
                                                    {"queryable_str_field", std::string{"foo"}},
                                                    {"embedded_obj", AnyDict{{"str_field", std::string{"bar"}}}}}));
        realm->commit_transaction();

        realm->begin_transaction();
        auto embedded_obj = util::any_cast<Object&&>(obj.get_property_value<util::Any>(c, "embedded_obj"));
        embedded_obj.set_property_value(c, "str_field", util::Any{std::string{"baz"}});
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);
    });
}

TEST_CASE("flx: uploading an object that is out-of-view results in compensating write", "[sync][flx][app]") {
    AppCreateConfig::FLXSyncRole role;
    role.name = "compensating_write_perms";
    role.read = true;
    role.write =
        nlohmann::json{{"queryable_str_field", nlohmann::json{{"$in", nlohmann::json::array({"foo", "bar"})}}}};
    FLXSyncTestHarness::ServerSchema server_schema{g_simple_embedded_obj_schema, {"queryable_str_field"}, {role}};
    FLXSyncTestHarness harness("flx_bad_query", server_schema);

    auto make_error_handler = [] {
        auto [error_promise, error_future] = util::make_promise_future<SyncError>();
        auto shared_promise = std::make_shared<decltype(error_promise)>(std::move(error_promise));
        auto fn = [error_promise = std::move(shared_promise)](std::shared_ptr<SyncSession>, SyncError err) mutable {
            if (!error_promise) {
                std::cerr << util::format(
                                 "An unexpected sync error was caught by the default SyncTestFile handler: '%1'",
                                 err.message)
                          << std::endl;
                abort();
            }
            std::move(error_promise)->emplace_value(std::move(err));
        };

        return std::make_pair(std::move(error_future), std::move(fn));
    };

    auto validate_sync_error = [&](const SyncError& sync_error, ObjectId invalid_obj,
                                   const std::string& error_msg_fragment) {
        CHECK(sync_error.error_code == sync::make_error_code(sync::ProtocolError::compensating_write));
        CHECK(sync_error.is_session_level_protocol_error());
        CHECK(!sync_error.is_client_reset_requested());
        CHECK(sync_error.compensating_writes_info.size() == 1);
        auto write_info = sync_error.compensating_writes_info[0];
        CHECK(write_info.primary_key.is_type(type_ObjectId));
        CHECK(write_info.primary_key.get_object_id() == invalid_obj);
        CHECK(write_info.object_name == "TopLevel");
        CHECK_THAT(write_info.reason, Catch::Matchers::ContainsSubstring(error_msg_fragment));
    };

    SECTION("compensating write because of permission violation") {
        harness.do_with_new_user([&](auto user) {
            SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
            auto&& [error_future, err_handler] = make_error_handler();
            config.sync_config->error_handler = err_handler;

            auto realm = Realm::get_shared_realm(config);
            auto table = realm->read_group().get_table("class_TopLevel");
            auto queryable_str_field = table->get_column_key("queryable_str_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table).equal(queryable_str_field, "bizz"));
            std::move(new_query).commit();

            CppContext c(realm);
            realm->begin_transaction();
            auto invalid_obj = ObjectId::gen();
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{{"_id", invalid_obj}, {"queryable_str_field", std::string{"bizz"}}}));
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            validate_sync_error(
                std::move(error_future).get(), invalid_obj,
                util::format("write to '%1' in table \"TopLevel\" not allowed", invalid_obj.to_string()));

            wait_for_advance(realm);

            auto top_level_table = realm->read_group().get_table("class_TopLevel");
            REQUIRE(top_level_table->is_empty());
        });
    }

    SECTION("compensating write because of permission violation with write on embedded object") {
        harness.do_with_new_user([&](auto user) {
            SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
            auto&& [error_future, err_handler] = make_error_handler();
            config.sync_config->error_handler = err_handler;

            auto realm = Realm::get_shared_realm(config);
            auto table = realm->read_group().get_table("class_TopLevel");
            auto queryable_str_field = table->get_column_key("queryable_str_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(
                Query(table).equal(queryable_str_field, "bizz").Or().equal(queryable_str_field, "foo"));
            std::move(new_query).commit();

            CppContext c(realm);
            realm->begin_transaction();
            auto invalid_obj = ObjectId::gen();
            auto obj =
                Object::create(c, realm, "TopLevel",
                               util::Any(AnyDict{{"_id", invalid_obj},
                                                 {"queryable_str_field", std::string{"foo"}},
                                                 {"embedded_obj", AnyDict{{"str_field", std::string{"bar"}}}}}));
            realm->commit_transaction();
            realm->begin_transaction();
            obj.set_property_value(c, "queryable_str_field", util::Any{std::string{"bizz"}});
            realm->commit_transaction();
            realm->begin_transaction();
            auto embedded_obj = util::any_cast<Object&&>(obj.get_property_value<util::Any>(c, "embedded_obj"));
            embedded_obj.set_property_value(c, "str_field", util::Any{std::string{"baz"}});
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);
            validate_sync_error(
                std::move(error_future).get(), invalid_obj,
                util::format("write to '%1' in table \"TopLevel\" not allowed", invalid_obj.to_string()));

            wait_for_advance(realm);

            obj = Object::get_for_primary_key(c, realm, "TopLevel", util::Any(invalid_obj));
            embedded_obj = util::any_cast<Object&&>(obj.get_property_value<util::Any>(c, "embedded_obj"));
            REQUIRE(util::any_cast<std::string&&>(obj.get_property_value<util::Any>(c, "queryable_str_field")) ==
                    "foo");
            REQUIRE(util::any_cast<std::string&&>(embedded_obj.get_property_value<util::Any>(c, "str_field")) ==
                    "bar");

            realm->begin_transaction();
            embedded_obj.set_property_value(c, "str_field", util::Any{std::string{"baz"}});
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            wait_for_advance(realm);
            obj = Object::get_for_primary_key(c, realm, "TopLevel", util::Any(invalid_obj));
            embedded_obj = util::any_cast<Object&&>(obj.get_property_value<util::Any>(c, "embedded_obj"));
            REQUIRE(util::any_cast<std::string&&>(embedded_obj.get_property_value<util::Any>(c, "str_field")) ==
                    "baz");
        });
    }

    SECTION("compensating write for writing a top-level object that is out-of-view") {
        harness.do_with_new_user([&](auto user) {
            SyncTestFile config(user, harness.schema(), SyncConfig::FLXSyncEnabled{});
            auto&& [error_future, err_handler] = make_error_handler();
            config.sync_config->error_handler = err_handler;

            auto realm = Realm::get_shared_realm(config);
            auto table = realm->read_group().get_table("class_TopLevel");
            auto queryable_str_field = table->get_column_key("queryable_str_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(new_query).commit();

            CppContext c(realm);
            realm->begin_transaction();
            auto valid_obj = ObjectId::gen();
            auto invalid_obj = ObjectId::gen();
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{
                               {"_id", valid_obj},
                               {"queryable_str_field", std::string{"foo"}},
                           }));
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{
                               {"_id", invalid_obj},
                               {"queryable_str_field", std::string{"bar"}},
                           }));
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            validate_sync_error(std::move(error_future).get(), invalid_obj,
                                "object is outside of the current query view");

            wait_for_advance(realm);

            auto top_level_table = realm->read_group().get_table("class_TopLevel");
            REQUIRE(top_level_table->size() == 1);
            REQUIRE(top_level_table->get_object_with_primary_key(valid_obj));

            realm->begin_transaction();
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{
                               {"_id", ObjectId::gen()},
                               {"queryable_str_field", std::string{"foo"}},
                           }));
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);
        });
    }
}

TEST_CASE("flx: query on non-queryable field results in query error message", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bad_query");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto bad_col_key = table->get_column_key("non_queryable_field");
        auto good_col_key = table->get_column_key("queryable_str_field");

        Query new_query_a(table);
        auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
        new_query_a.equal(bad_col_key, "bar");
        new_subs.insert_or_assign(new_query_a);
        auto subs = std::move(new_subs).commit();
        auto sub_res = subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get_no_throw();
        CHECK(!sub_res.is_ok());
        if (sub_res.get_status().reason().find("Client provided query with bad syntax:") == std::string::npos ||
            sub_res.get_status().reason().find(
                "\"TopLevel\": key \"non_queryable_field\" is not a queryable field") == std::string::npos) {
            FAIL(sub_res.get_status().reason());
        }

        CHECK(realm->get_active_subscription_set().version() == 0);
        CHECK(realm->get_latest_subscription_set().version() == 1);

        Query new_query_b(table);
        new_query_b.equal(good_col_key, "foo");
        new_subs = realm->get_active_subscription_set().make_mutable_copy();
        new_subs.insert_or_assign(new_query_b);
        subs = std::move(new_subs).commit();
        subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();

        CHECK(realm->get_active_subscription_set().version() == 2);
        CHECK(realm->get_latest_subscription_set().version() == 2);
    });
}

TEST_CASE("flx: interrupted bootstrap restarts/recovers on reconnect", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bootstrap_batching", {g_large_array_schema, {"queryable_int_field"}});

    std::vector<ObjectId> obj_ids_at_end = fill_large_array_schema(harness);
    SyncTestFile interrupted_realm_config(harness.app()->current_user(), harness.schema(),
                                          SyncConfig::FLXSyncEnabled{});
    interrupted_realm_config.cache = false;

    {
        auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
        Realm::Config config = interrupted_realm_config;
        config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
        auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
        config.sync_config->on_download_message_received_hook = [promise = std::move(shared_promise)](
                                                                    std::weak_ptr<SyncSession> weak_session,
                                                                    const sync::SyncProgress&, int64_t query_version,
                                                                    sync::DownloadBatchState batch_state) mutable {
            auto session = weak_session.lock();
            if (!session) {
                return;
            }

            auto latest_subs = session->get_flx_subscription_store()->get_latest();
            if (latest_subs.version() == 1 && latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping) {
                REQUIRE(query_version == 1);
                REQUIRE(batch_state == sync::DownloadBatchState::MoreToCome);
                session->close();
                promise->emplace_value();
            }
        };

        auto realm = Realm::get_shared_realm(config);
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            auto table = realm->read_group().get_table("class_TopLevel");
            mut_subs.insert_or_assign(Query(table));
            std::move(mut_subs).commit();
        }

        interrupted.get();
        realm->sync_session()->shutdown_and_wait();
        realm->close();
    }

    _impl::RealmCoordinator::clear_all_caches();

    {
        auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path);
        auto sub_store = sync::SubscriptionStore::create(realm, [](int64_t) {});
        REQUIRE(sub_store->get_active_and_latest_versions() == std::pair<int64_t, int64_t>{0, 1});
        auto latest_subs = sub_store->get_latest();
        REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping);
        REQUIRE(latest_subs.size() == 1);
        REQUIRE(latest_subs.at(0).object_class_name() == "TopLevel");
    }

    auto realm = Realm::get_shared_realm(interrupted_realm_config);
    auto table = realm->read_group().get_table("class_TopLevel");
    realm->get_latest_subscription_set().get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    wait_for_upload(*realm);
    wait_for_download(*realm);

    wait_for_advance(realm);
    REQUIRE(table->size() == obj_ids_at_end.size());
    for (auto& id : obj_ids_at_end) {
        REQUIRE(table->find_primary_key(Mixed{id}));
    }

    auto active_subs = realm->get_active_subscription_set();
    auto latest_subs = realm->get_latest_subscription_set();
    REQUIRE(active_subs.version() == latest_subs.version());
    REQUIRE(active_subs.version() == int64_t(1));
}

TEST_CASE("flx: dev mode uploads schema before query change", "[sync][flx][app]") {
    FLXSyncTestHarness::ServerSchema server_schema;
    auto default_schema = FLXSyncTestHarness::default_server_schema();
    server_schema.queryable_fields = default_schema.queryable_fields;
    server_schema.dev_mode_enabled = true;
    server_schema.schema = Schema{};

    FLXSyncTestHarness harness("flx_dev_mode", server_schema);
    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            // auto queryable_str_field = table->get_column_key("queryable_str_field");
            // auto queryable_int_field = table->get_column_key("queryable_int_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table));
            std::move(new_query).commit();

            CppContext c(realm);
            realm->begin_transaction();
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{{"_id", foo_obj_id},
                                             {"queryable_str_field", std::string{"foo"}},
                                             {"queryable_int_field", static_cast<int64_t>(5)},
                                             {"non_queryable_field", std::string{"non queryable 1"}}}));
            Object::create(c, realm, "TopLevel",
                           util::Any(AnyDict{{"_id", bar_obj_id},
                                             {"queryable_str_field", std::string{"bar"}},
                                             {"queryable_int_field", static_cast<int64_t>(10)},
                                             {"non_queryable_field", std::string{"non queryable 2"}}}));
            realm->commit_transaction();

            wait_for_upload(*realm);
        },
        default_schema.schema);

    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            auto queryable_int_field = table->get_column_key("queryable_int_field");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table).greater_equal(queryable_int_field, int64_t(5)));
            auto subs = std::move(new_query).commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
            wait_for_download(*realm);
            Results results(realm, table);

            realm->refresh();
            CHECK(results.size() == 2);
            CHECK(table->get_object_with_primary_key({foo_obj_id}).is_valid());
            CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
        },
        default_schema.schema);
}

TEST_CASE("flx: writes work offline", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_offline_writes");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto sync_session = realm->sync_session();
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto queryable_int_field = table->get_column_key("queryable_int_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(Query(table));
        std::move(new_query).commit();

        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();

        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", foo_obj_id},
                                         {"queryable_str_field", std::string{"foo"}},
                                         {"queryable_int_field", static_cast<int64_t>(5)},
                                         {"non_queryable_field", std::string{"non queryable 1"}}}));
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", bar_obj_id},
                                         {"queryable_str_field", std::string{"bar"}},
                                         {"queryable_int_field", static_cast<int64_t>(10)},
                                         {"non_queryable_field", std::string{"non queryable 2"}}}));
        realm->commit_transaction();

        wait_for_upload(*realm);
        wait_for_download(*realm);
        sync_session->close();

        // Make it so the subscriptions only match the "foo" object
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(mut_subs).commit();
        }

        // Make foo so that it will match the next subscription update. This checks whether you can do
        // multiple subscription set updates offline and that the last one eventually takes effect when
        // you come back online and fully synchronize.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        // Update our subscriptions so that both foo/bar will be included
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            std::move(mut_subs).commit();
        }

        // Make foo out of view for the current subscription.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 0);
            realm->commit_transaction();
        }

        sync_session->revive_if_needed();
        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        Results results(realm, table);
        CHECK(results.size() == 1);
        CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
    });
}

TEST_CASE("flx: writes work without waiting for sync", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_offline_writes");

    harness.do_with_new_realm([&](SharedRealm realm) {
        auto table = realm->read_group().get_table("class_TopLevel");
        auto queryable_str_field = table->get_column_key("queryable_str_field");
        auto queryable_int_field = table->get_column_key("queryable_int_field");
        auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
        new_query.insert_or_assign(Query(table));
        std::move(new_query).commit();

        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();

        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", foo_obj_id},
                                         {"queryable_str_field", std::string{"foo"}},
                                         {"queryable_int_field", static_cast<int64_t>(5)},
                                         {"non_queryable_field", std::string{"non queryable 1"}}}));
        Object::create(c, realm, "TopLevel",
                       util::Any(AnyDict{{"_id", bar_obj_id},
                                         {"queryable_str_field", std::string{"bar"}},
                                         {"queryable_int_field", static_cast<int64_t>(10)},
                                         {"non_queryable_field", std::string{"non queryable 2"}}}));
        realm->commit_transaction();

        wait_for_upload(*realm);

        // Make it so the subscriptions only match the "foo" object
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).equal(queryable_str_field, "foo"));
            std::move(mut_subs).commit();
        }

        // Make foo so that it will match the next subscription update. This checks whether you can do
        // multiple subscription set updates without waiting and that the last one eventually takes effect when
        // you fully synchronize.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 15);
            realm->commit_transaction();
        }

        // Update our subscriptions so that both foo/bar will be included
        {
            auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
            mut_subs.clear();
            mut_subs.insert_or_assign(Query(table).greater_equal(queryable_int_field, static_cast<int64_t>(10)));
            std::move(mut_subs).commit();
        }

        // Make foo out-of-view for the current subscription.
        {
            Results results(realm, table);
            realm->begin_transaction();
            auto foo_obj = table->get_object_with_primary_key(Mixed{foo_obj_id});
            foo_obj.set<int64_t>(queryable_int_field, 0);
            realm->commit_transaction();
        }

        wait_for_upload(*realm);
        wait_for_download(*realm);

        realm->refresh();
        Results results(realm, table);
        CHECK(results.size() == 1);
        CHECK(table->get_object_with_primary_key({bar_obj_id}).is_valid());
    });
}

TEST_CASE("flx: subscriptions persist after closing/reopening", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bad_query");
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});

    {
        auto orig_realm = Realm::get_shared_realm(config);
        auto mut_subs = orig_realm->get_latest_subscription_set().make_mutable_copy();
        mut_subs.insert_or_assign(Query(orig_realm->read_group().get_table("class_TopLevel")));
        std::move(mut_subs).commit();
        orig_realm->close();
    }

    {
        auto new_realm = Realm::get_shared_realm(config);
        auto latest_subs = new_realm->get_latest_subscription_set();
        CHECK(latest_subs.size() == 1);
        latest_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
    }
}

TEST_CASE("flx: no subscription store created for PBS app", "[sync][flx][app]") {
    const std::string base_url = get_base_url();
    auto server_app_config = minimal_app_config(base_url, "flx_connect_as_pbs", g_minimal_schema);
    TestAppSession session(create_app(server_app_config));
    SyncTestFile config(session.app(), bson::Bson{}, g_minimal_schema);

    auto realm = Realm::get_shared_realm(config);
    CHECK(!wait_for_download(*realm));
    CHECK(!wait_for_upload(*realm));

    CHECK(!realm->sync_session()->get_flx_subscription_store());
}

TEST_CASE("flx: connect to FLX as PBS returns an error", "[sync][flx][app]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");
    SyncTestFile config(harness.app(), bson::Bson{}, harness.schema());
    std::mutex sync_error_mutex;
    util::Optional<SyncError> sync_error;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) mutable {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        sync_error = std::move(error);
    };
    auto realm = Realm::get_shared_realm(config);
    timed_wait_for([&] {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        return static_cast<bool>(sync_error);
    });

    CHECK(sync_error->error_code == make_error_code(sync::ProtocolError::switch_to_flx_sync));
}

TEST_CASE("flx: connect to FLX with partition value returns an error", "[sync][flx][app]") {
    FLXSyncTestHarness harness("connect_to_flx_as_pbs");
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    config.sync_config->partition_value = "\"foobar\"";

    CHECK_THROWS_AS(Realm::get_shared_realm(config), std::logic_error);
}

TEST_CASE("flx: connect to PBS as FLX returns an error", "[sync][flx][app]") {
    const std::string base_url = get_base_url();

    auto server_app_config = minimal_app_config(base_url, "flx_connect_as_pbs", g_minimal_schema);
    TestAppSession session(create_app(server_app_config));
    auto app = session.app();
    auto user = app->current_user();

    SyncTestFile config(user, g_minimal_schema, SyncConfig::FLXSyncEnabled{});

    std::mutex sync_error_mutex;
    util::Optional<SyncError> sync_error;
    config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) mutable {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        sync_error = std::move(error);
    };
    auto realm = Realm::get_shared_realm(config);
    auto latest_subs = realm->get_latest_subscription_set().make_mutable_copy();
    auto table = realm->read_group().get_table("class_TopLevel");
    Query new_query_a(table);
    new_query_a.equal(table->get_column_key("_id"), ObjectId::gen());
    latest_subs.insert_or_assign(std::move(new_query_a));
    std::move(latest_subs).commit();

    timed_wait_for([&] {
        std::lock_guard<std::mutex> lk(sync_error_mutex);
        return static_cast<bool>(sync_error);
    });

    CHECK(sync_error->error_code == make_error_code(sync::ProtocolError::switch_to_pbs));
}

TEST_CASE("flx: commit subscription while refreshing the access token", "[sync][flx][app]") {
    class HookedTransport : public SynchronousTestTransport {
    public:
        void send_request_to_server(Request&& request,
                                    util::UniqueFunction<void(const Response&)>&& completion_block) override
        {
            if (request_hook) {
                request_hook(request);
            }
            SynchronousTestTransport::send_request_to_server(std::move(request), [&](const Response& response) {
                completion_block(response);
            });
        }
        util::UniqueFunction<void(Request&)> request_hook;
    };

    auto transport = std::make_shared<HookedTransport>();
    FLXSyncTestHarness harness("flx_wait_access_token2", FLXSyncTestHarness::default_server_schema(), transport);
    auto app = harness.app();
    std::shared_ptr<SyncUser> user = app->current_user();
    REQUIRE(user);
    REQUIRE(!user->access_token_refresh_required());
    // Set a bad access token, with an expired time. This will trigger a refresh initiated by the client.
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    using namespace std::chrono_literals;
    auto expires = std::chrono::system_clock::to_time_t(now - 30s);
    user->update_access_token(encode_fake_jwt("fake_access_token", expires));
    REQUIRE(user->access_token_refresh_required());

    bool seen_waiting_for_access_token = false;
    // Commit a subcription set while there is no sync session.
    // A session is created when the access token is refreshed.
    transport->request_hook = [&](Request&) {
        auto user = app->current_user();
        REQUIRE(user);
        for (auto& session : user->all_sessions()) {
            if (session->state() == SyncSession::State::WaitingForAccessToken) {
                REQUIRE(!seen_waiting_for_access_token);
                seen_waiting_for_access_token = true;

                auto store = session->get_flx_subscription_store();
                REQUIRE(store);
                auto mut_subs = store->get_latest().make_mutable_copy();
                std::move(mut_subs).commit();
            }
        }
    };
    SyncTestFile config(harness.app()->current_user(), harness.schema(), SyncConfig::FLXSyncEnabled{});
    // This triggers the token refresh.
    auto r = Realm::get_shared_realm(config);
    REQUIRE(seen_waiting_for_access_token);
}

TEST_CASE("flx: bootstrap batching prevents orphan documents", "[sync][flx][app]") {
    FLXSyncTestHarness harness("flx_bootstrap_batching", {g_large_array_schema, {"queryable_int_field"}});

    std::vector<ObjectId> obj_ids_at_end = fill_large_array_schema(harness);
    SyncTestFile interrupted_realm_config(harness.app()->current_user(), harness.schema(),
                                          SyncConfig::FLXSyncEnabled{});
    interrupted_realm_config.cache = false;

    auto check_interrupted_state = [&](const DBRef& realm) {
        auto tr = realm->start_read();
        auto top_level = tr->get_table("class_TopLevel");
        REQUIRE(top_level);
        REQUIRE(top_level->is_empty());

        auto sub_store = sync::SubscriptionStore::create(realm, [](int64_t) {});
        REQUIRE(sub_store->get_active_and_latest_versions() == std::pair<int64_t, int64_t>{0, 1});
        auto latest_subs = sub_store->get_latest();
        REQUIRE(latest_subs.state() == sync::SubscriptionSet::State::Bootstrapping);
        REQUIRE(latest_subs.size() == 1);
        REQUIRE(latest_subs.at(0).object_class_name() == "TopLevel");
    };

    auto mutate_realm = [&] {
        harness.load_initial_data([&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            wait_for_advance(realm);
            Results res(realm, Query(table).greater(table->get_column_key("queryable_int_field"), int64_t(10)));
            REQUIRE(res.size() == 2);
            res.clear();
        });
    };

    SECTION("interrupted before final bootstrap message") {
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
            config.sync_config->on_bootstrap_message_processed_hook =
                [promise = std::move(shared_promise)](std::weak_ptr<SyncSession> weak_session,
                                                      const sync::SyncProgress&, int64_t query_version,
                                                      sync::DownloadBatchState batch_state) mutable {
                    auto session = weak_session.lock();
                    if (!session) {
                        return true;
                    }

                    if (query_version == 1 && batch_state == sync::DownloadBatchState::MoreToCome) {
                        session->close();
                        promise->emplace_value();
                        return false;
                    }
                    return true;
                };
            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                std::move(mut_subs).commit();
            }

            interrupted.get();
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        _impl::RealmCoordinator::clear_all_caches();

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path);
            util::StderrLogger logger;
            sync::PendingBootstrapStore bootstrap_store(realm, &logger);
            REQUIRE(bootstrap_store.has_pending());
            auto pending_batch = bootstrap_store.peek_pending(1024 * 1024 * 16);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(!pending_batch.progress);
            REQUIRE(pending_batch.remaining == 0);
            REQUIRE(pending_batch.changesets.size() == 1);

            check_interrupted_state(realm);
        }

        // Now we'll open a different realm and make some changes that would leave orphan objects on the client
        // if the bootstrap batches weren't being cached until lastInBatch were true.
        mutate_realm();

        // Finally re-open the realm whose bootstrap we interrupted and just wait for it to finish downloading.
        auto realm = Realm::get_shared_realm(interrupted_realm_config);
        auto table = realm->read_group().get_table("class_TopLevel");
        realm->get_latest_subscription_set()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get();
        wait_for_upload(*realm);
        wait_for_download(*realm);

        wait_for_advance(realm);
        auto expected_obj_ids = util::Span<ObjectId>(obj_ids_at_end).sub_span(0, 3);

        REQUIRE(table->size() == expected_obj_ids.size());
        for (auto& id : expected_obj_ids) {
            REQUIRE(table->find_primary_key(Mixed{id}));
        }
    }

    SECTION("interrupted after final bootstrap message before processing") {
        {
            auto [interrupted_promise, interrupted] = util::make_promise_future<void>();
            Realm::Config config = interrupted_realm_config;
            config.sync_config = std::make_shared<SyncConfig>(*interrupted_realm_config.sync_config);
            auto shared_promise = std::make_shared<util::Promise<void>>(std::move(interrupted_promise));
            config.sync_config->on_bootstrap_message_processed_hook =
                [promise = std::move(shared_promise)](std::weak_ptr<SyncSession> weak_session,
                                                      const sync::SyncProgress, int64_t query_version,
                                                      sync::DownloadBatchState batch_state) mutable {
                    auto session = weak_session.lock();
                    if (!session) {
                        return true;
                    }

                    if (query_version == 1 && batch_state == sync::DownloadBatchState::LastInBatch) {
                        session->close();
                        promise->emplace_value();
                        return false;
                    }
                    return true;
                };
            auto realm = Realm::get_shared_realm(config);
            {
                auto mut_subs = realm->get_latest_subscription_set().make_mutable_copy();
                auto table = realm->read_group().get_table("class_TopLevel");
                mut_subs.insert_or_assign(Query(table));
                std::move(mut_subs).commit();
            }

            interrupted.get();
            realm->sync_session()->shutdown_and_wait();
            realm->close();
        }

        _impl::RealmCoordinator::clear_all_caches();

        // Open up the realm without the sync client attached and verify that the realm got interrupted in the state
        // we expected it to be in.
        {
            auto realm = DB::create(sync::make_client_replication(), interrupted_realm_config.path);
            util::StderrLogger logger;
            sync::PendingBootstrapStore bootstrap_store(realm, &logger);
            REQUIRE(bootstrap_store.has_pending());
            auto pending_batch = bootstrap_store.peek_pending(1024 * 1024 * 16);
            REQUIRE(pending_batch.query_version == 1);
            REQUIRE(static_cast<bool>(pending_batch.progress));
            REQUIRE(pending_batch.remaining == 0);
            REQUIRE(pending_batch.changesets.size() == 3);

            check_interrupted_state(realm);
        }

        // Now we'll open a different realm and make some changes that would leave orphan objects on the client
        // if the bootstrap batches weren't being cached until lastInBatch were true.
        mutate_realm();

        auto [saw_valid_state_promise, saw_valid_state_future] = util::make_promise_future<void>();
        auto shared_saw_valid_state_promise =
            std::make_shared<decltype(saw_valid_state_promise)>(std::move(saw_valid_state_promise));
        SharedRealm realm;
        // This hook will let us check what the state of the realm is before it's integrated any new download
        // messages from the server. This should be the full 5 object bootstrap that was received before we
        // called mutate_realm().
        interrupted_realm_config.sync_config->on_download_message_received_hook =
            [&, promise = std::move(shared_saw_valid_state_promise)](std::weak_ptr<SyncSession> weak_session,
                                                                     const sync::SyncProgress&, int64_t query_version,
                                                                     sync::DownloadBatchState batch_state) {
                auto session = weak_session.lock();
                if (!session) {
                    return;
                }

                if (query_version != 1 || batch_state != sync::DownloadBatchState::LastInBatch) {
                    return;
                }

                auto latest_sub_set = session->get_flx_subscription_store()->get_latest();
                auto active_sub_set = session->get_flx_subscription_store()->get_active();
                REQUIRE(latest_sub_set.version() == active_sub_set.version());
                REQUIRE(active_sub_set.state() == sync::SubscriptionSet::State::Complete);

                auto db = SyncSession::OnlyForTesting::get_db(*session);
                auto tr = db->start_read();

                auto table = tr->get_table("class_TopLevel");
                REQUIRE(table->size() == obj_ids_at_end.size());
                for (auto& id : obj_ids_at_end) {
                    REQUIRE(table->find_primary_key(Mixed{id}));
                }

                promise->emplace_value();
            };

        // Finally re-open the realm whose bootstrap we interrupted and just wait for it to finish downloading.
        realm = Realm::get_shared_realm(interrupted_realm_config);
        saw_valid_state_future.get();
        auto table = realm->read_group().get_table("class_TopLevel");
        realm->get_latest_subscription_set()
            .get_state_change_notification(sync::SubscriptionSet::State::Complete)
            .get();
        wait_for_upload(*realm);
        wait_for_download(*realm);
        wait_for_advance(realm);

        auto expected_obj_ids = util::Span<ObjectId>(obj_ids_at_end).sub_span(0, 3);

        // After we've downloaded all the mutations there should only by 3 objects left.
        REQUIRE(table->size() == expected_obj_ids.size());
        for (auto& id : expected_obj_ids) {
            REQUIRE(table->find_primary_key(Mixed{id}));
        }
    }
}

TEST_CASE("flx: asymmetric sync", "[sync][flx][app]") {
    static auto server_schema = [] {
        FLXSyncTestHarness::ServerSchema server_schema;
        server_schema.queryable_fields = {"queryable_str_field"};
        server_schema.schema = {
            {"Asymmetric",
             ObjectSchema::ObjectType::TopLevelAsymmetric,
             {
                 {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                 {"location", PropertyType::String | PropertyType::Nullable},
                 {"embedded_obj", PropertyType::Object | PropertyType::Nullable, "Embedded"},
             }},
            {"Embedded",
             ObjectSchema::ObjectType::Embedded,
             {
                 {"value", PropertyType::String | PropertyType::Nullable},
             }},
        };
        return server_schema;
    }();
    static auto harness = std::make_unique<FLXSyncTestHarness>("asymmetric_sync", server_schema);

    SECTION("basic object construction") {
        auto foo_obj_id = ObjectId::gen();
        auto bar_obj_id = ObjectId::gen();
        harness->do_with_new_realm([&](SharedRealm realm) {
            realm->begin_transaction();
            CppContext c(realm);
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", foo_obj_id}, {"location", std::string{"foo"}}}));
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", bar_obj_id}, {"location", std::string{"bar"}}}));
            realm->commit_transaction();
        });

        harness->do_with_new_realm([&](SharedRealm realm) {
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            // Cannot query asymmetric tables.
            CHECK_THROWS_AS(new_query.insert_or_assign(Query(table)), LogicError);
        });
    }

    SECTION("do not allow objects with same key within the same transaction") {
        auto foo_obj_id = ObjectId::gen();
        harness->do_with_new_realm([&](SharedRealm realm) {
            realm->begin_transaction();
            CppContext c(realm);
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", foo_obj_id}, {"location", std::string{"foo"}}}));
            CHECK_THROWS_WITH(
                Object::create(c, realm, "Asymmetric",
                               util::Any(AnyDict{{"_id", foo_obj_id}, {"location", std::string{"bar"}}})),
                "Attempting to create an object of type 'Asymmetric' with an existing primary key value 'not "
                "implemented'.");
            realm->commit_transaction();
        });

        harness->do_with_new_realm([&](SharedRealm realm) {
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
        });
    }

    SECTION("replace object") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            auto foo_obj_id = ObjectId::gen();
            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", foo_obj_id}, {"location", std::string{"foo"}}}));
            realm->commit_transaction();
            realm->begin_transaction();
            // Update `location` field.
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", foo_obj_id}, {"location", std::string{"bar"}}}));
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
        });
    }

    SECTION("create multiple objects - separate commits") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            for (int i = 0; i < 100; ++i) {
                realm->begin_transaction();
                auto obj_id = ObjectId::gen();
                Object::create(c, realm, "Asymmetric",
                               util::Any(AnyDict{{"_id", obj_id}, {"location", util::format("foo_%1", i)}}));
                realm->commit_transaction();
            }

            wait_for_upload(*realm);
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
        });
    }

    SECTION("create multiple objects - same commit") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            realm->begin_transaction();
            for (int i = 0; i < 100; ++i) {
                auto obj_id = ObjectId::gen();
                Object::create(c, realm, "Asymmetric",
                               util::Any(AnyDict{{"_id", obj_id}, {"location", util::format("foo_%1", i)}}));
            }
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
        });
    }

    SECTION("open with schema mismatch on IsAsymmetric") {
        auto schema = server_schema.schema;
        schema.find("Asymmetric")->table_type = ObjectSchema::ObjectType::TopLevel;

        harness->do_with_new_user([&](std::shared_ptr<SyncUser> user) {
            SyncTestFile config(user, schema, SyncConfig::FLXSyncEnabled{});
            std::condition_variable cv;
            std::mutex wait_mutex;
            bool wait_flag(false);
            std::error_code ec;
            config.sync_config->error_handler = [&](std::shared_ptr<SyncSession>, SyncError error) {
                std::unique_lock<std::mutex> lock(wait_mutex);
                wait_flag = true;
                ec = error.error_code;
                cv.notify_one();
            };

            auto realm = Realm::get_shared_realm(config);

            std::unique_lock<std::mutex> lock(wait_mutex);
            cv.wait(lock, [&wait_flag]() {
                return wait_flag == true;
            });
            CHECK(ec.value() == int(realm::sync::ClientError::bad_changeset));
        });
    }

    SECTION("basic embedded object construction") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            realm->begin_transaction();
            CppContext c(realm);
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", ObjectId::gen()},
                                             {"embedded_obj", AnyDict{{"value", std::string{"foo"}}}}}));
            realm->commit_transaction();
        });

        harness->do_with_new_realm([&](SharedRealm realm) {
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
        });
    }

    SECTION("replace embedded object") {
        harness->do_with_new_realm([&](SharedRealm realm) {
            CppContext c(realm);
            auto foo_obj_id = ObjectId::gen();
            realm->begin_transaction();
            Object::create(
                c, realm, "Asymmetric",
                util::Any(AnyDict{{"_id", foo_obj_id}, {"embedded_obj", AnyDict{{"value", std::string{"foo"}}}}}));
            realm->commit_transaction();
            // Update embedded field to `null`.
            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", foo_obj_id}, {"embedded_obj", util::Any()}}));
            realm->commit_transaction();
            // Update embedded field again to a new value.
            realm->begin_transaction();
            Object::create(
                c, realm, "Asymmetric",
                util::Any(AnyDict{{"_id", foo_obj_id}, {"embedded_obj", AnyDict{{"value", std::string{"bar"}}}}}));
            realm->commit_transaction();

            wait_for_upload(*realm);
            wait_for_download(*realm);

            auto table = realm->read_group().get_table("class_Asymmetric");
            REQUIRE(table->size() == 0);
        });
    }

    // Add any new test sections above this point

    SECTION("teardown") {
        harness.reset();
    }
}

TEST_CASE("flx: asymmetric sync - dev mode", "[sync][flx][app]") {
    FLXSyncTestHarness::ServerSchema server_schema;
    server_schema.dev_mode_enabled = true;
    server_schema.schema = Schema{};

    auto schema = Schema{{"Asymmetric",
                          ObjectSchema::ObjectType::TopLevelAsymmetric,
                          {
                              {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                              {"location", PropertyType::String | PropertyType::Nullable},
                          }},
                         {"TopLevel",
                          {
                              {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                              {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                          }}};

    FLXSyncTestHarness harness("asymmetric_sync", server_schema);

    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();

    harness.do_with_new_realm(
        [&](SharedRealm realm) {
            auto table = realm->read_group().get_table("class_TopLevel");
            auto new_query = realm->get_latest_subscription_set().make_mutable_copy();
            new_query.insert_or_assign(Query(table));
            std::move(new_query).commit();

            CppContext c(realm);
            realm->begin_transaction();
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", foo_obj_id}, {"location", std::string{"foo"}}}));
            Object::create(c, realm, "Asymmetric",
                           util::Any(AnyDict{{"_id", bar_obj_id}, {"location", std::string{"bar"}}}));
            realm->commit_transaction();

            wait_for_upload(*realm);
        },
        schema);
}

} // namespace realm::app

#endif // REALM_ENABLE_AUTH_TESTS
