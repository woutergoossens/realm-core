// swift-tools-version:5.2

import PackageDescription
import Foundation

let versionStr = "10.5.0"
let versionPieces = versionStr.split(separator: "-")
let versionCompontents = versionPieces[0].split(separator: ".")
let versionExtra = versionPieces.count > 1 ? versionPieces[1] : ""

let cxxSettings: [CXXSetting] = [
    .headerSearchPath("src"),
    .define("REALM_DEBUG", .when(configuration: .debug)),
    .define("REALM_NO_CONFIG"),
    .define("REALM_INSTALL_LIBEXECDIR", to: ""),
    .define("REALM_ENABLE_ASSERTIONS", to: "1"),
    .define("REALM_ENABLE_ENCRYPTION", to: "1"),

    .define("REALM_VERSION_MAJOR", to: String(versionCompontents[0])),
    .define("REALM_VERSION_MINOR", to: String(versionCompontents[1])),
    .define("REALM_VERSION_PATCH", to: String(versionCompontents[2])),
    .define("REALM_VERSION_EXTRA", to: "\"\(versionExtra)\""),
    .define("REALM_VERSION_STRING", to: "\"\(versionStr)\""),
    .define("REALM_NOINST_ROOT_CERTS", to: "0"),
    .define("REALM_HAVE_UV", to: "1", .when(platforms: [.linux]))
]

var syncServerSources = [
    "realm/sync/encrypt",
    "realm/sync/noinst/file_descriptors.cpp",
    "realm/sync/noinst/reopening_file_logger.cpp",
    "realm/sync/noinst/server_dir.cpp",
    "realm/sync/noinst/server_file_access_cache.cpp",
    "realm/sync/noinst/server_history.cpp",
    "realm/sync/noinst/server_legacy_migration.cpp",
    "realm/sync/noinst/vacuum.cpp",
    "realm/sync/access_control.cpp",
    "realm/sync/metrics.cpp",
    "realm/sync/server_configuration.cpp",
    "realm/sync/server.cpp"
]

#if os(Linux)
syncServerSources.append("realm/sync/crypto_server_openssl.cpp")
#else
syncServerSources.append("realm/sync/crypto_server_apple.mm")
#endif

let syncCommandSources = [
    "realm/sync/apply_to_state_command.cpp",
    "realm/sync/encrypt/encryption_transformer_command.cpp",
    "realm/sync/inspector",
    "realm/sync/noinst/vacuum_command.cpp",
    "realm/sync/dump_command.cpp",
    "realm/sync/hist_command.cpp",
    "realm/sync/print_changeset_command.cpp",
    "realm/sync/realm_upgrade.cpp",
    "realm/sync/server_command.cpp",
    "realm/sync/server_index.cpp",
    "realm/sync/server_index_command.cpp",
    "realm/sync/stat_command.cpp",
    "realm/sync/verify_server_file_command.cpp"
]

// MARK: SyncClient Exclusions
var syncClientExcludes = syncServerSources + syncCommandSources
#if os(Linux)
syncClientExcludes.append("realm/sync/crypto_server_apple.mm")
#else
syncClientExcludes.append("realm/sync/crypto_server_openssl.cpp")
#endif

// MARK: ObjectStore Exclusions
var objectStoreExcludes = [
    "realm/object-store/util/generic",
    "realm/object-store/impl/windows",
    "realm/object-store/c_api",
    "realm/object-store/impl/generic",
    "realm/object-store/bson/bson.cpp" // needed by sync client
]
#if os(Linux)
objectStoreExcludes.append("realm/object-store/impl/apple/keychain_helper.cpp")
objectStoreExcludes.append("realm/object-store/impl/apple/external_commit_helper.cpp")
#else
objectStoreExcludes.append("realm/object-store/impl/epoll/external_commit_helper.cpp")
#endif

// MARK: CoreTests Exclusions
var coreTestsExcludes = [
    "bench",
    "bench-sync",
    "benchmark-common-tasks",
    "benchmark-common-tasks-ios",
    "benchmark-crud",
    "benchmark-history-types",
    "benchmark-index",
    "benchmark-insert-add",
    "benchmark-prealloc",
    "benchmark-row-accessor",
    "benchmark-transaction",
    "benchmark-util-network",
    "client",
    "cloud_perf_test",
    "compat",
    "csv_test",
    "experiments",
    "fuzz",
    "fuzzy",
    "ios",
    "large_tests",
    "object-store",
    "performance",
    "protocol_compat",
    "simple-connection",
    "unit-tests-ios",
    // Disabled tests
    "test_sync.cpp",
    "test_sync_multiserver.cpp",
    "test_metrics.cpp",
    "test_client_reset.cpp",
    "test_client_reset_diff.cpp",
    "test_client_reset_query_based.cpp",
    "test_handshake.cpp",
    "test_noinst_vacuum.cpp",
]

#if os(Linux)
coreTestsExcludes.append("main.mm")
#else
coreTestsExcludes.append("main.cpp")
#endif

var coreTestsTarget = PackageDescription.Target.target(
    name: "CoreTests",
    dependencies: [
        "SyncServer",
        "ObjectStore", // needed for bson symbols
        "QueryParser"
    ],
    path: "test",
    exclude: coreTestsExcludes,
    cxxSettings: ([
        .define("REALM_ENABLE_SYNC", to: "1"),
        .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
        .define("REALM_HAVE_UV", to: "1", .when(platforms: [.linux])),
        .headerSearchPath("."),
        .headerSearchPath("../external/catch/single_include"),
    ] + cxxSettings) as [CXXSetting],
    linkerSettings: [
      .linkedLibrary("pthread", .when(platforms: [.linux])),
      .linkedLibrary("uv", .when(platforms: [.linux])),
      .linkedLibrary("m", .when(platforms: [.linux])),
      .linkedLibrary("crypto", .when(platforms: [.linux]))
    ]
)
#if swift(>=5.3.1) && !os(Linux)
//coreTestsTarget.resources = [
//    .copy("resources"),
//    .copy("stitch_private.pem"),
//    .copy("stitch_public.pem"),
//    .copy("test_pubkey.pem"),
//    .copy("test_pubkey2.pem"),
//    .copy("test_sync_ca.pem"),
//    .copy("test_sync_key.pem"),
//    .copy("test_token_expiration_null.json"),
//    .copy("test_token_expiration_specified.json"),
//    .copy("test_token_expiration_unspecified.json"),
//    .copy("test_token_for_path.json"),
//    .copy("test_token_readonly.json"),
//    .copy("test_token_sync_label_custom.json"),
//    .copy("test_token_sync_label_default.json"),
//    .copy("test_token.json"),
//
//    .copy("test_upgrade_colkey_error.realm"),
//    .copy("test_upgrade_database_4_1.realm"),
//    .copy("test_upgrade_database_4_2.realm"),
//    .copy("test_upgrade_database_4_3.realm"),
//    .copy("test_upgrade_database_4_4_to_5_datetime1.realm"),
//    .copy("test_upgrade_database_4_4.realm"),
//    .copy("test_upgrade_database_4_5_to_6_stringindex.realm"),
//    .copy("test_upgrade_database_4_6_to_7.realm"),
//    .copy("test_upgrade_database_4_7_to_8.realm"),
//    .copy("test_upgrade_database_4_8_to_9.realm"),
//    .copy("test_upgrade_database_4_9_to_10.realm"),
//    .copy("test_upgrade_database_4_10_to_11.realm"),
//    .copy("test_upgrade_database_6.realm"),
//    .copy("test_upgrade_database_9_to_10_pk_table.realm"),
//    .copy("test_upgrade_database_1000_1.realm"),
//    .copy("test_upgrade_database_1000_2.realm"),
//    .copy("test_upgrade_database_1000_3.realm"),
//    .copy("test_upgrade_database_1000_4_to_5_datetime1.realm"),
//    .copy("test_upgrade_database_1000_4.realm"),
//    .copy("test_upgrade_database_1000_5_to_6_stringindex.realm"),
//    .copy("test_upgrade_database_1000_6_to_7.realm"),
//    .copy("test_upgrade_database_1000_7_to_8.realm"),
//    .copy("test_upgrade_database_1000_8_to_9.realm"),
//    .copy("test_upgrade_database_1000_9_to_10.realm"),
//    .copy("test_upgrade_database_1000_10_to_11.realm"),
//    .copy("test_upgrade_progress_1.realm"),
//    .copy("test_upgrade_progress_2.realm"),
//    .copy("test_upgrade_progress_3.realm"),
//    .copy("test_upgrade_progress_4.realm"),
//    .copy("test_upgrade_progress_5.realm"),
//    .copy("test_upgrade_progress_6.realm"),
//    .copy("test_upgrade_progress_7.realm"),
//
//    .copy("test_util_network_ssl_ca.pem"),
//    .copy("test_util_network_ssl_key.pem"),
//
//    .copy("test.pem"),
//    .copy("expect_json.json"),
//    .copy("expect_string.txt"),
//    .copy("expect_test_upgrade_database_9_to_10.json"),
//    .copy("expect_xjson_plus.json"),
//    .copy("expect_xjson.json"),
//    .copy("expected_json_link_cycles1.json"),
//    .copy("expected_json_link_cycles2.json"),
//    .copy("expected_json_link_cycles3.json"),
//    .copy("expected_json_link_cycles4.json"),
//    .copy("expected_json_link_cycles5.json"),
//    .copy("expected_json_linklist_cycle1.json"),
//    .copy("expected_json_linklist_cycle2.json"),
//    .copy("expected_json_linklist_cycle3.json"),
//    .copy("expected_json_linklist_cycle4.json"),
//    .copy("expected_json_linklist_cycle5.json"),
//    .copy("expected_json_linklist_cycle6.json"),
//    .copy("expected_json_linklist1_1.json"),
//    .copy("expected_json_linklist1_2.json"),
//    .copy("expected_json_linklist1_3.json"),
//    .copy("expected_json_linklist1_4.json"),
//    .copy("expected_json_linklist1_5.json"),
//    .copy("expected_json_linklist1_6.json"),
//    .copy("expected_json_nulls.json"),
//    .copy("expected_xjson_link.json"),
//    .copy("expected_xjson_linklist1.json"),
//    .copy("expected_xjson_linklist2.json"),
//    .copy("expected_xjson_plus_link.json"),
//    .copy("expected_xjson_plus_linklist1.json"),
//    .copy("expected_xjson_plus_linklist2.json"),
//]
#endif

let purCapi = PackageDescription.Target.target(
    name: "PureCapi",
    dependencies: ["Capi"],
    path: "src",
    sources: ["realm/object-store/c_api/realm.c"],
    publicHeadersPath: "realm.h",
    cSettings: [.headerSearchPath(".")]
)
let package = Package(
    name: "RealmDatabase",
    platforms: [
        .macOS(.v10_10),
        .iOS(.v11),
        .tvOS(.v9),
        .watchOS(.v2)
    ],
    products: [
        .library(
            name: "RealmStorage",
            targets: ["Storage"]),
        .library(
            name: "RealmQueryParser",
            type: .dynamic,
            targets: ["QueryParser"]),
        .library(
            name: "RealmSyncClient",
            type: .dynamic,
            targets: ["SyncClient"]
        ),
        .library(
            name: "RealmObjectStore",
            type: .dynamic,
            targets: ["ObjectStore"]),
        .library(
            name: "RealmCapi",
            targets: ["Capi"]),
        .library(
            name: "RealmFFI",
            targets: ["RealmFFI"]),
        .executable(name: "RealmObjectStoreTests", targets: ["ObjectStoreTests"])
    ],
    targets: [
        .target(
            name: "Bid",
            path: "src/external/IntelRDFPMathLib20U2/LIBRARY/src",
            sources: [
                "bid128.c",
                "bid128_compare.c",
                "bid128_mul.c",
                "bid128_div.c",
                "bid128_add.c",
                "bid128_fma.c",
                "bid64_to_bid128.c",
                "bid_convert_data.c",
                "bid_decimal_data.c",
                "bid_decimal_globals.c",
                "bid_from_int.c",
                "bid_round.c"
            ],
            publicHeadersPath: "."
        ),
        .target(
            name: "Storage",
            dependencies: ["Bid"],
            path: "src",
            exclude: [
                "realm/tools",
                "realm/parser",
                "realm/metrics",
                "realm/exec",
                "realm/object-store",
                "realm/sync",
                "external",
                "win32",
                "realm/util/network.cpp",
                "realm/util/network_ssl.cpp",
                "realm/util/http.cpp",
                "realm/util/websocket.cpp",
                "realm/realm.h"
            ],
            sources: [
                "realm"
            ],
            publicHeadersPath: "realm",
            cxxSettings: cxxSettings + [.headerSearchPath(".")],
            linkerSettings: [
                .linkedFramework("CoreFoundation", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS]))
            ]
        ),
        .target(
            name: "QueryParser",
            dependencies: ["Storage"],
            path: "src",
            sources: ["realm/parser"],
            publicHeadersPath: "realm/parser",
            cxxSettings: [
                .headerSearchPath("."),
                .headerSearchPath("realm/parser/generated"),
                .headerSearchPath("realm/parser")
            ] + cxxSettings),
        .target(
            name: "SyncClient",
            dependencies: ["Storage"],
            path: "src",
            exclude: syncClientExcludes,
            sources: [
                "realm/sync",
                "realm/util/network.cpp",
                "realm/util/network_ssl.cpp",
                "realm/util/http.cpp",
                "realm/util/websocket.cpp",
                "realm/object-store/bson/bson.cpp"
            ],
            publicHeadersPath: "realm/sync",
            cxxSettings: [
                .headerSearchPath("."),
                .define("REALM_HAVE_SECURE_TRANSPORT", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
            ] + cxxSettings,
            linkerSettings: [
                .linkedFramework("Security", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .linkedLibrary("z")
            ]),
        /**
        .target(
            name: "ExternalCommitHelper",
            dependencies: ["Storage"],
            path: "src",
            sources: [
                "realm/object-store/impl/apple"
            ],
            publicHeadersPath: "realm/object-store/impl/apple",
            cxxSettings: [
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS]))
            ] + cxxSettings
        ),
        .target(
            name: "ExternalCommitHelperLinux",
            dependencies: ["Storage"],
            path: "src",
            sources: [
                "realm/object-store/impl/epoll"
            ],
            publicHeadersPath: "realm/object-store/impl/epoll",
            cxxSettings: [
                .define("REALM_HAVE_EPOLL", to: "1", .when(platforms: [.linux])),
            ] + cxxSettings
        ),*/
        .target(
            name: "ObjectStore",
            dependencies: [
                "SyncClient",
//                ._targetItem(name: "ExternalCommitHelper",
//                             condition: .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
 //               ._targetItem(name: "ExternalCommitHelperLinux",
  //                           condition: .when(platforms: [.linux])),
            ],
            path: "src",
            exclude: objectStoreExcludes,
            sources: ["realm/object-store"],
            publicHeadersPath: "realm/object-store",
            cxxSettings: ([
                .headerSearchPath("."),
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .define("REALM_HAVE_EPOLL", to: "1", .when(platforms: [.linux])),
                .define("REALM_HAVE_UV", to: "1", .when(platforms: [.linux])),
//                .headerSearchPath("realm/object-store/**/*"),
//                .headerSearchPath("realm/object-store/impl")
            ] + cxxSettings) as [CXXSetting],
            linkerSettings: [
                .linkedFramework("Security", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .linkedLibrary("z")
            ]),
        .target(
            name: "Capi",
            dependencies: ["ObjectStore", "QueryParser"],
            path: "src",
            exclude: [
                "realm/object-store/c_api/realm.c"
            ],
            sources: ["realm/object-store/c_api"],
            publicHeadersPath: "realm/object-store/c_api",
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .headerSearchPath("."),
                .headerSearchPath("realm/object-store/c_api"),
                .headerSearchPath("external/pegtl/include/tao")
                          ] + cxxSettings) as [CXXSetting],
            linkerSettings: [
              .linkedLibrary("pthread", .when(platforms: [.linux])),
              .linkedLibrary("uv", .when(platforms: [.linux])),
              .linkedLibrary("m", .when(platforms: [.linux])),
              .linkedLibrary("crypto", .when(platforms: [.linux]))
            ]),
        .target(
            name: "PureCapi",
            dependencies: ["Capi"],
            path: "src",
            sources: ["realm/object-store/c_api/realm.c"],
            publicHeadersPath: "include",
            cSettings: [.headerSearchPath(".")]
        ),
        .target(
            name: "RealmFFI",
            dependencies: ["PureCapi"],
            path: "src",
            sources: ["swift"]
//            publicHeadersPath: "."
        ),
        /*.target(
            name: "ObjectStoreTestUtils",
            dependencies: ["ObjectStore"],
            path: "test/object-store/util",
            publicHeadersPath: ".",
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .headerSearchPath(".."),
                .headerSearchPath("../../../external/catch/single_include"),
            ] + cxxSettings) as [CXXSetting]),
        */.target(
            name: "SyncServer",
            dependencies: [
                "SyncClient"
            ],
            path: "src",
            exclude: syncCommandSources,
            sources: syncServerSources,
            publicHeadersPath: "realm/sync/impl", // hack
            cxxSettings: cxxSettings + [
                .headerSearchPath(".")
            ],
            linkerSettings: [
                .linkedFramework("Foundation", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
            ]),
        .target(
            name: "SyncCommand",
            dependencies: [
                "SyncClient"
            ],
            path: "src",
            exclude: syncServerSources,
            sources: syncCommandSources,
            publicHeadersPath: "realm/sync/impl", // hack
            cxxSettings: cxxSettings,
            linkerSettings: [
                .linkedFramework("Foundation", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
            ]),
        coreTestsTarget,
        .target(
            name: "ObjectStoreTests",
            dependencies: [
              //"SyncClient",
              "ObjectStore",
              "QueryParser",
              "SyncServer",
              //"ObjectStoreTestUtils"
            ],
            path: "test/object-store",
            exclude: [
                "benchmarks",
                "notifications-fuzzer",
                "c_api",
                //"util"
            ],
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .define("REALM_HAVE_UV", to: "1", .when(platforms: [.linux])),
                .headerSearchPath("."),
                .headerSearchPath("../../src"),
                .headerSearchPath("../../external/catch/single_include"),
                          ] + cxxSettings) as [CXXSetting],
            linkerSettings: [
              .linkedLibrary("pthread", .when(platforms: [.linux])),
              .linkedLibrary("uv", .when(platforms: [.linux])),
              .linkedLibrary("m", .when(platforms: [.linux])),
              .linkedLibrary("crypto", .when(platforms: [.linux]))
            ]
        ),
/*        .target(
            name: "CapiTests",
            dependencies: ["Capi",
                           "ObjectStoreTestUtils",
                           "SyncServer"],
            path: "test/object-store/c_api",
            cxxSettings: ([
                .define("REALM_ENABLE_SYNC", to: "1"),
                .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
                .headerSearchPath("../"),
                .headerSearchPath("../../../external/catch/single_include")
            ] + cxxSettings) as [CXXSetting])*/
    ],
    cxxLanguageStandard: .cxx1z
)
