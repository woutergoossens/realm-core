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

#include <catch2/catch.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include "util/baas_admin_api.hpp"
#include "util/event_loop.hpp"
#include "util/test_utils.hpp"
#include "util/test_file.hpp"

#include <external/json/json.hpp>
#include <realm/sync/access_token.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/websocket.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/instance.hpp>

#include <chrono>
#include <future>
#include <thread>
#include <iostream>

namespace realm::app {
using bsoncxx::builder::basic::kvp;
using util::any_cast;
using util::Optional;
using namespace std::string_view_literals;

// temporarily disable these tests for now,
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

#if REALM_ENABLE_AUTH_TESTS
mongocxx::instance mongo_client_instance = {};

class IntTestTransport : public GenericNetworkTransport {
public:
    void send_request_to_server(const Request request, std::function<void(const Response)> completion_block) override
    {
        completion_block(do_http_request(request));
    }
};

TEST_CASE("app: qbs connection", "[sync][app]") {
    auto factory = []() -> std::unique_ptr<GenericNetworkTransport> {
        return std::make_unique<IntTestTransport>();
    };
    std::string base_url = get_base_url();
    const std::string valid_pk_name = "_id";
    REQUIRE(!base_url.empty());

    Schema schema{
        ObjectSchema("TopLevel",
                     {
                         {valid_pk_name, PropertyType::ObjectId, Property::IsPrimary{true}},
                         {"queryable_str_field", PropertyType::String | PropertyType::Nullable},
                         {"queryable_int_field", PropertyType::Int | PropertyType::Nullable},
                         {"non_queryable_field", PropertyType::String | PropertyType::Nullable},
                     }),
    };

    auto server_app_config = minimal_app_config(base_url, "qbs_enabled", schema);
    server_app_config.query_based_sync_enabled = true;
    auto app_session = create_app(server_app_config);

    struct ToInsert {
        ToInsert(int64_t int_field, std::string str_field, std::string non_queryable)
            : id(ObjectId::gen())
            , int_field(int_field)
            , str_field(std::move(str_field))
            , non_queryable(std::move(non_queryable))
        {
        }

        ObjectId id;
        int64_t int_field;
        std::string str_field;
        std::string non_queryable;
    };

    std::vector<ToInsert> to_insert_models = {
        {25, "foo", "model1"},
        {50, "bar", "model2"},
        {75, "bizz", "model3"},
        {100, "buzz", "model4"},
    };
    {
        mongocxx::client mongo_client(mongocxx::uri("mongodb://localhost:26000"));
        auto state_store = mongo_client.database(util::format("__realm_sync_%1", app_session.server_app_id));
        auto top_level_state = state_store.collection("state_TopLevel");

        std::vector<bsoncxx::document::value> to_insert;
        for (const auto& model : to_insert_models) {
            bsoncxx::document::value doc =
                bsoncxx::builder::stream::document{}
                << "_id" << bsoncxx::oid(model.id.to_string()) << "queryable_int_field" << model.int_field
                << "queryable_str_field" << model.str_field << "non_queryable_field" << model.non_queryable
                << "_sync_meta" << bsoncxx::builder::stream::open_document << "latestVersion" << int64_t(1)
                << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize;
            to_insert.push_back(std::move(doc));
        }
        auto insert_res = top_level_state.insert_many(to_insert);
        CHECK(insert_res);
        CHECK(static_cast<size_t>(insert_res->inserted_count()) == to_insert_models.size());
    }

    auto app_config = App::Config{app_session.client_app_id,
                                  factory,
                                  base_url,
                                  util::none,
                                  Optional<std::string>("A Local App Version"),
                                  util::none,
                                  "Object Store Platform Tests",
                                  "Object Store Platform Version Blah",
                                  "An sdk version"};

    auto base_path = util::make_temp_dir() + app_config.app_id;
    util::try_remove_dir_recursive(base_path);
    util::try_make_dir(base_path);

    auto make_realm_config = [&](const std::shared_ptr<SyncUser> user, std::string path) {
        realm::Realm::Config realm_config;
        realm_config.sync_config = std::make_shared<realm::SyncConfig>(user, bson::Bson("foo"));
        realm_config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        realm_config.sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
            std::cout << error.message << std::endl;
        };

        realm_config.sync_config->partition_value = "";

        realm_config.schema_version = 1;
        realm_config.path = base_path + path;
        realm_config.schema = server_app_config.schema;
        return realm_config;
    };

    auto email = util::format("realm_tests_do_autoverify-test@example.com");
    auto password = std::string{"password"};

    {
        TestSyncManager::Config sync_manager_config(app_config);
        sync_manager_config.verbose_sync_client_logging = true;
        sync_manager_config.enable_query_based_sync = true;
        TestSyncManager sync_manager(sync_manager_config, {});
        auto app = sync_manager.app();
        app->provider_client<App::UsernamePasswordProviderClient>().register_email(
            email, password, [&](Optional<app::AppError> error) {
                CHECK(!error);
            });
        app->log_in_with_credentials(realm::app::AppCredentials::username_password(email, password),
                                     [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                         REQUIRE(user);
                                         CHECK(!error);
                                     });
        auto realm_config = make_realm_config(app->current_user(), "realm1");
        realm_config.sync_config->query_value =
            std::string{R"({"TopLevel": {"queryable_int_field": { "$gt": 50 }}})"};

        auto realm = realm::Realm::get_shared_realm(std::move(realm_config));
        auto session = app->current_user()->session_for_on_disk_path(realm->config().path);

        std::promise<void> promise;
        auto future = promise.get_future();
        auto shared_promise = std::make_shared<std::promise<void>>(std::move(promise));
        session->wait_for_download_completion(
            [shared_promise = std::move(shared_promise)](std::error_code ec) mutable {
                REALM_ASSERT(!ec);
                shared_promise->set_value();
            });

        future.wait();

        realm::Results found_models(realm, realm->read_group().get_table("class_TopLevel"));
        CHECK(found_models.size() == static_cast<size_t>(2));
    }

    {
        TestSyncManager::Config sync_manager_config(app_config);
        sync_manager_config.verbose_sync_client_logging = true;
        sync_manager_config.enable_query_based_sync = true;
        TestSyncManager sync_manager(sync_manager_config, {});
        auto app = sync_manager.app();
        app->log_in_with_credentials(realm::app::AppCredentials::username_password(email, password),
                                     [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
                                         REQUIRE(user);
                                         CHECK(!error);
                                     });
        auto realm_config = make_realm_config(app->current_user(), "realm2");
        realm_config.sync_config->query_value =
            std::string{R"({"TopLevel": {"queryable_str_field": { "$in": [ "foo", "bizz", "buzz" ] }}})"};

        auto realm = realm::Realm::get_shared_realm(std::move(realm_config));
        auto session = app->current_user()->session_for_on_disk_path(realm->config().path);

        std::promise<void> promise;
        auto future = promise.get_future();
        auto shared_promise = std::make_shared<std::promise<void>>(std::move(promise));
        session->wait_for_download_completion(
            [shared_promise = std::move(shared_promise)](std::error_code ec) mutable {
                REALM_ASSERT(!ec);
                shared_promise->set_value();
            });

        future.wait();

        realm::Results found_models(realm, realm->read_group().get_table("class_TopLevel"));
        CHECK(found_models.size() == static_cast<size_t>(3));
    }
}

#endif

} // namespace realm::app
