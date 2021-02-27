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
    "realm/object-store/util/bson" // needed by sync client
]
#if os(Linux)
objectStoreExcludes.append("realm/object-store/impl/apple/keychain_helper.cpp")
objectStoreExcludes.append("realm/object-store/impl/apple/external_commit_helper.cpp")
#else
objectStoreExcludes.append("realm/object-store/impl/epoll/external_commit_helper.cpp")
#endif

let purCapi = PackageDescription.Target.target(
    name: "PureCapi",
    dependencies: ["Capi"],
    path: "src",
    sources: ["realm/object-store/c_api/realm.c"],
    publicHeadersPath: "realm.h",
    cSettings: [.headerSearchPath(".")]
)

//            exclude: [
//                "realm/tools",
//                "realm/parser",
//                "realm/metrics",
//                "realm/exec",
//                "realm/object-store",
//                "realm/sync",
//                "external",
//                "win32",
//                "realm/util/network.cpp",
//                "realm/util/network_ssl.cpp",
//                "realm/util/http.cpp",
//                "realm/util/websocket.cpp",
//                "realm/realm.h"
//            ],

let bid = PackageDescription.Target.target(
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
)

let realmCore = PackageDescription.Target.target(
    name: "RealmStorage",
    dependencies: ["Bid"],
    path: "src",
    exclude: [
        "realm/object-store/c_api",
        "realm/exec",
        "realm/metrics",
        "realm/tools",
        "win32",
        "external",
        "realm/realm.h"
    ] + syncClientExcludes + objectStoreExcludes,
    sources: [
        "realm"
    ],
    publicHeadersPath: "realm",
    cxxSettings: cxxSettings + [
        .headerSearchPath("."),
        .define("REALM_ENABLE_SYNC", to: "1"),
        .define("REALM_PLATFORM_APPLE", to: "1", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
        .define("REALM_HAVE_EPOLL", to: "1", .when(platforms: [.linux])),
        .define("REALM_HAVE_UV", to: "1", .when(platforms: [.linux])),
    ],
    linkerSettings: [
        .linkedFramework("CoreFoundation", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
        .linkedFramework("Security", .when(platforms: [.macOS, .iOS, .tvOS, .watchOS])),
        .linkedLibrary("z")
    ]
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
            targets: ["RealmStorage"]),
        .library(
            name: "PureCapi",
            targets: ["PureCapi"]),
        .library(
            name: "RealmFFI",
            targets: ["RealmFFI"])
    ],
    targets: [
        bid,
        realmCore,
        .target(
            name: "RealmCapi",
            dependencies: ["RealmStorage"],
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
            dependencies: ["RealmCapi"],
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
        )
    ],
    cxxLanguageStandard: .cxx1z
)
