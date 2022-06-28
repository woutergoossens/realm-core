////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#ifndef REALM_SYNC_TEST_UTILS_HPP
#define REALM_SYNC_TEST_UTILS_HPP

#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/object-store/sync/impl/sync_file.hpp>
#include <realm/object-store/sync/impl/sync_metadata.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/util/functional.hpp>
#include <realm/util/function_ref.hpp>

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

// disable the tests that rely on having baas available on the network
// but allow opt-in by building with REALM_ENABLE_AUTH_TESTS=1
#ifndef REALM_ENABLE_AUTH_TESTS
#define REALM_ENABLE_AUTH_TESTS 0
#endif

namespace realm {

bool results_contains_user(SyncUserMetadataResults& results, const std::string& identity,
                           const std::string& auth_server);
bool results_contains_original_name(SyncFileActionMetadataResults& results, const std::string& original_name);

void timed_wait_for(util::FunctionRef<bool()> condition,
                    std::chrono::milliseconds max_ms = std::chrono::milliseconds(5000));

void timed_sleeping_wait_for(util::FunctionRef<bool()> condition,
                             std::chrono::milliseconds max_ms = std::chrono::seconds(30));

struct ExpectedRealmPaths {
    ExpectedRealmPaths(const std::string& base_path, const std::string& app_id, const std::string& user_identity,
                       const std::string& local_identity, const std::string& partition);
    std::string current_preferred_path;
    std::string fallback_hashed_path;
    std::string legacy_local_id_path;
    std::string legacy_sync_path;
    std::vector<std::string> legacy_sync_directories_to_make;
};

#if REALM_ENABLE_SYNC

template <typename Transport>
const std::shared_ptr<app::GenericNetworkTransport> instance_of = std::make_shared<Transport>();

std::ostream& operator<<(std::ostream& os, util::Optional<app::AppError> error);

template <typename Transport>
TestSyncManager::Config get_config(Transport&& transport)
{
    TestSyncManager::Config config;
    config.transport = transport;
    return config;
}

#if REALM_ENABLE_AUTH_TESTS

#ifdef REALM_MONGODB_ENDPOINT
std::string get_base_url();
#endif

struct AutoVerifiedEmailCredentials : app::AppCredentials {
    AutoVerifiedEmailCredentials();
    std::string email;
    std::string password;
};

AutoVerifiedEmailCredentials create_user_and_log_in(app::SharedApp app);

#endif // REALM_ENABLE_AUTH_TESTS

#endif // REALM_ENABLE_SYNC

namespace reset_utils {

struct Partition {
    std::string property_name;
    std::string value;
};

Obj create_object(Realm& realm, StringData object_type, util::Optional<ObjectId> primary_key = util::none,
                  util::Optional<Partition> partition = util::none);

struct TestClientReset {
    using Callback = util::UniqueFunction<void(const SharedRealm&)>;
    TestClientReset(const Realm::Config& local_config, const Realm::Config& remote_config);
    virtual ~TestClientReset();
    TestClientReset* setup(Callback&& on_setup);
    TestClientReset* make_local_changes(Callback&& changes_local);
    TestClientReset* make_remote_changes(Callback&& changes_remote);
    TestClientReset* on_post_local_changes(Callback&& post_local);
    TestClientReset* on_post_reset(Callback&& post_reset);
    void set_pk_of_object_driving_reset(const ObjectId& pk);
    ObjectId get_pk_of_object_driving_reset() const;

    virtual void run() = 0;

protected:
    realm::Realm::Config m_local_config;
    realm::Realm::Config m_remote_config;

    Callback m_on_setup;
    Callback m_make_local_changes;
    Callback m_make_remote_changes;
    Callback m_on_post_local;
    Callback m_on_post_reset;
    bool m_did_run = false;
    ObjectId m_pk_driving_reset = ObjectId::gen();
};

#if REALM_ENABLE_SYNC

#if REALM_ENABLE_AUTH_TESTS
std::unique_ptr<TestClientReset> make_baas_client_reset(const Realm::Config& local_config,
                                                        const Realm::Config& remote_config,
                                                        TestAppSession& test_app_session);

std::unique_ptr<TestClientReset> make_baas_flx_client_reset(const Realm::Config& local_config,
                                                            const Realm::Config& remote_config,
                                                            const TestAppSession& test_app_session);
#endif // REALM_ENABLE_AUTH_TESTS

#endif // REALM_ENABLE_SYNC

std::unique_ptr<TestClientReset> make_fake_local_client_reset(const Realm::Config& local_config,
                                                              const Realm::Config& remote_config);

} // namespace reset_utils

} // namespace realm

#endif // REALM_SYNC_TEST_UTILS_HPP
