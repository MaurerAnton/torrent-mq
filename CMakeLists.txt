cmake_minimum_required(VERSION 3.20)
project(torrent-mq VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---- Build options ----
option(TORRENT_BUILD_TESTS "Build tests" ON)
option(TORRENT_BUILD_BENCHMARKS "Build benchmarks" ON)
option(TORRENT_BUILD_CLI "Build CLI tools" ON)
option(TORRENT_USE_GRPC "Use gRPC for networking" ON)
option(TORRENT_USE_ROCKSDB "Use RocksDB for storage" ON)
option(TORRENT_USE_PROMETHEUS "Enable Prometheus metrics" ON)
option(TORRENT_ENABLE_SANITIZERS "Enable address/UB sanitizers" OFF)
option(TORRENT_ENABLE_LTO "Enable link-time optimization" OFF)
option(TORRENT_BUILD_SHARED "Build as shared library" ON)

# ---- FetchContent ----
include(FetchContent)

# nlohmann/json
FetchContent_Declare(nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

# fmt
FetchContent_Declare(fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG 11.0.2
)
FetchContent_MakeAvailable(fmt)

# spdlog
FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.15.1
)
FetchContent_MakeAvailable(spdlog)

# Google Test (if tests enabled)
if(TORRENT_BUILD_TESTS)
    FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.15.2
    )
    FetchContent_MakeAvailable(googletest)
endif()

# Benchmark (if benchmarks enabled)
if(TORRENT_BUILD_BENCHMARKS)
    FetchContent_Declare(benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.9.1
    )
    FetchContent_MakeAvailable(benchmark)
endif()

# ---- System dependencies ----
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

# Boost (CONFIG mode for Boost 1.91+)
find_package(Boost CONFIG REQUIRED COMPONENTS filesystem thread)
# Boost.System is part of filesystem in 1.91+; no separate component

# ---- Protobuf & gRPC ----
if(TORRENT_USE_GRPC)
    find_package(Protobuf REQUIRED)
    find_package(gRPC CONFIG REQUIRED)
endif()

# ---- RocksDB ----
if(TORRENT_USE_ROCKSDB)
    find_package(RocksDB REQUIRED)
endif()

# ---- Prometheus ----
if(TORRENT_USE_PROMETHEUS)
    FetchContent_Declare(prometheus-cpp
        GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
        GIT_TAG v1.3.0
    )
    set(ENABLE_PUSH OFF CACHE BOOL "" FORCE)
    set(ENABLE_COMPRESSION OFF CACHE BOOL "" FORCE)
    set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(prometheus-cpp)
endif()

# ---- Source files ----
set(TORRENT_SOURCES
    # Core broker
    src/torrent/broker/server.cpp
    src/torrent/broker/topic_manager.cpp
    src/torrent/broker/partition_manager.cpp
    src/torrent/broker/subscription_manager.cpp
    src/torrent/broker/consumer_group_manager.cpp
    src/torrent/broker/request_dispatcher.cpp
    src/torrent/broker/inter_broker.cpp
    src/torrent/broker/controller.cpp
    src/torrent/broker/quota_manager.cpp
    src/torrent/broker/retention_manager.cpp
    src/torrent/broker/compaction_manager.cpp
    src/torrent/broker/transaction_coordinator.cpp
    src/torrent/broker/leader_balancer.cpp
    src/torrent/broker/dlm.cpp

    # Storage engine
    src/torrent/storage/segment.cpp
    src/torrent/storage/segment_index.cpp
    src/torrent/storage/log_manager.cpp
    src/torrent/storage/log_segment.cpp
    src/torrent/storage/offset_index.cpp
    src/torrent/storage/time_index.cpp
    src/torrent/storage/compaction.cpp
    src/torrent/storage/cleaner.cpp
    src/torrent/storage/page_cache.cpp
    src/torrent/storage/batch_cache.cpp
    src/torrent/storage/disk_io.cpp
    src/torrent/storage/snapshot.cpp
    src/torrent/storage/recovery.cpp
    src/torrent/storage/rocksdb_store.cpp
    src/torrent/storage/file_store.cpp
    src/torrent/storage/tiered_storage.cpp
    src/torrent/storage/archival.cpp
    src/torrent/storage/encryption.cpp

    # Network layer
    src/torrent/network/acceptor.cpp
    src/torrent/network/connection.cpp
    src/torrent/network/connection_pool.cpp
    src/torrent/network/tcp_transport.cpp
    src/torrent/network/tls_transport.cpp
    src/torrent/network/grpc_server.cpp
    src/torrent/network/grpc_client.cpp
    src/torrent/network/protocol_parser.cpp
    src/torrent/network/protocol_serializer.cpp
    src/torrent/network/request_handler.cpp
    src/torrent/network/response_handler.cpp
    src/torrent/network/message_codec.cpp
    src/torrent/network/flow_control.cpp
    src/torrent/network/connection_limiter.cpp
    src/torrent/network/throttle.cpp
    src/torrent/network/sasl_handshake.cpp

    # Consensus (Raft)
    src/torrent/consensus/raft_node.cpp
    src/torrent/consensus/raft_log.cpp
    src/torrent/consensus/raft_sm.cpp
    src/torrent/consensus/raft_rpc.cpp
    src/torrent/consensus/raft_snapshot.cpp
    src/torrent/consensus/raft_election.cpp
    src/torrent/consensus/raft_heartbeat.cpp
    src/torrent/consensus/raft_membership.cpp
    src/torrent/consensus/raft_leadership.cpp
    src/torrent/consensus/raft_fsm.cpp
    src/torrent/consensus/raft_prevote.cpp
    src/torrent/consensus/leader_lease.cpp
    src/torrent/consensus/metadata_manager.cpp
    src/torrent/consensus/controller_sm.cpp

    # Client protocol
    src/torrent/client/produce_handler.cpp
    src/torrent/client/fetch_handler.cpp
    src/torrent/client/offset_handler.cpp
    src/torrent/client/metadata_handler.cpp
    src/torrent/client/group_handler.cpp
    src/torrent/client/admin_handler.cpp
    src/torrent/client/describe_handler.cpp
    src/torrent/client/alter_handler.cpp
    src/torrent/client/delete_handler.cpp
    src/torrent/client/sasl_handler.cpp
    src/torrent/client/api_versions.cpp

    # Admin REST API
    src/torrent/admin/admin_server.cpp
    src/torrent/admin/cluster_handler.cpp
    src/torrent/admin/topic_handler.cpp
    src/torrent/admin/consumer_handler.cpp
    src/torrent/admin/broker_handler.cpp
    src/torrent/admin/metrics_handler.cpp
    src/torrent/admin/config_handler.cpp
    src/torrent/admin/acl_handler.cpp
    src/torrent/admin/quota_handler.cpp
    src/torrent/admin/rebalance_handler.cpp
    src/torrent/admin/health_handler.cpp

    # Security
    src/torrent/security/auth_manager.cpp
    src/torrent/security/acl_engine.cpp
    src/torrent/security/tls_manager.cpp
    src/torrent/security/sasl_provider.cpp
    src/torrent/security/kerberos.cpp
    src/torrent/security/oauth2.cpp
    src/torrent/security/mtls.cpp
    src/torrent/security/audit_log.cpp
    src/torrent/security/encryption_at_rest.cpp
    src/torrent/security/delegation_token.cpp

    # Metrics & monitoring
    src/torrent/metrics/prometheus_exporter.cpp
    src/torrent/metrics/broker_metrics.cpp
    src/torrent/metrics/topic_metrics.cpp
    src/torrent/metrics/consumer_metrics.cpp
    src/torrent/metrics/network_metrics.cpp
    src/torrent/metrics/storage_metrics.cpp
    src/torrent/metrics/jmx_exporter.cpp
    src/torrent/metrics/latency_histogram.cpp
    src/torrent/metrics/throughput_tracker.cpp
    src/torrent/metrics/alert_manager.cpp

    # Schema registry
    src/torrent/schema/schema_registry.cpp
    src/torrent/schema/avro_provider.cpp
    src/torrent/schema/protobuf_provider.cpp
    src/torrent/schema/json_schema_provider.cpp
    src/torrent/schema/compatibility_checker.cpp
    src/torrent/schema/schema_cache.cpp
    src/torrent/schema/subject_manager.cpp
    src/torrent/schema/serde.cpp

    # Stream processing
    src/torrent/streams/stream_processor.cpp
    src/torrent/streams/join_processor.cpp
    src/torrent/streams/filter_processor.cpp
    src/torrent/streams/aggregate_processor.cpp
    src/torrent/streams/window_processor.cpp
    src/torrent/streams/table_processor.cpp
    src/torrent/streams/topology_builder.cpp
    src/torrent/streams/state_store.cpp
    src/torrent/streams/punctuator.cpp
    src/torrent/streams/repartition.cpp

    # Connectors
    src/torrent/connectors/source_connector.cpp
    src/torrent/connectors/sink_connector.cpp
    src/torrent/connectors/connect_framework.cpp
    src/torrent/connectors/task_manager.cpp
    src/torrent/connectors/offset_backing_store.cpp
    src/torrent/connectors/transforms.cpp
    src/torrent/connectors/debezium_source.cpp
    src/torrent/connectors/jdbc_sink.cpp
    src/torrent/connectors/s3_sink.cpp
    src/torrent/connectors/elasticsearch_sink.cpp

    # Proxy (REST, WebSocket, MQTT)
    src/torrent/proxy/rest_proxy.cpp
    src/torrent/proxy/websocket_proxy.cpp
    src/torrent/proxy/mqtt_bridge.cpp
    src/torrent/proxy/amqp_bridge.cpp
    src/torrent/proxy/http_bridge.cpp
    src/torrent/proxy/nats_bridge.cpp

    # CLI tools
    src/torrent/cli/torrentctl.cpp
    src/torrent/cli/topic_cmd.cpp
    src/torrent/cli/consumer_cmd.cpp
    src/torrent/cli/producer_cmd.cpp
    src/torrent/cli/cluster_cmd.cpp
    src/torrent/cli/acl_cmd.cpp
    src/torrent/cli/config_cmd.cpp
    src/torrent/cli/mirror_cmd.cpp
    src/torrent/cli/perf_test.cpp
    src/torrent/cli/offset_cmd.cpp
    src/torrent/cli/doctor_cmd.cpp

    # Common utilities
    src/torrent/common/config.cpp
    src/torrent/common/signal_handler.cpp
    src/torrent/common/thread_pool.cpp
    src/torrent/common/timer.cpp
    src/torrent/common/rate_limiter.cpp
    src/torrent/common/crc32.cpp
    src/torrent/common/xxhash.cpp
    src/torrent/common/uuid.cpp
    src/torrent/common/endian.cpp
    src/torrent/common/varint.cpp
    src/torrent/common/iobuf.cpp
    src/torrent/common/backoff.cpp
    src/torrent/common/retry.cpp
    src/torrent/common/circular_buffer.cpp
    src/torrent/common/lru_cache.cpp
    src/torrent/common/concurrent_map.cpp
    src/torrent/common/semaphore.cpp
    src/torrent/common/pidfile.cpp
    src/torrent/common/hostname.cpp
    src/torrent/common/network_util.cpp
)

# ---- Headers ----
set(TORRENT_HEADERS
    include/torrent/torrent.h
    include/torrent/broker/server.h
    include/torrent/broker/topic_manager.h
    include/torrent/broker/partition_manager.h
    include/torrent/broker/subscription_manager.h
    include/torrent/broker/consumer_group_manager.h
    include/torrent/broker/request_dispatcher.h
    include/torrent/broker/inter_broker.h
    include/torrent/broker/controller.h
    include/torrent/broker/quota_manager.h
    include/torrent/broker/retention_manager.h
    include/torrent/broker/compaction_manager.h
    include/torrent/broker/transaction_coordinator.h
    include/torrent/broker/leader_balancer.h
    include/torrent/broker/dlm.h
    include/torrent/storage/segment.h
    include/torrent/storage/segment_index.h
    include/torrent/storage/log_manager.h
    include/torrent/storage/log_segment.h
    include/torrent/storage/offset_index.h
    include/torrent/storage/time_index.h
    include/torrent/storage/compaction.h
    include/torrent/storage/cleaner.h
    include/torrent/storage/page_cache.h
    include/torrent/storage/batch_cache.h
    include/torrent/storage/disk_io.h
    include/torrent/storage/snapshot.h
    include/torrent/storage/recovery.h
    include/torrent/storage/types.h
    include/torrent/network/acceptor.h
    include/torrent/network/connection.h
    include/torrent/network/connection_pool.h
    include/torrent/network/transport.h
    include/torrent/network/protocol.h
    include/torrent/network/message_codec.h
    include/torrent/network/flow_control.h
    include/torrent/network/connection_limiter.h
    include/torrent/network/throttle.h
    include/torrent/network/sasl_handshake.h
    include/torrent/consensus/raft.h
    include/torrent/consensus/raft_types.h
    include/torrent/consensus/metadata_manager.h
    include/torrent/client/produce_handler.h
    include/torrent/client/fetch_handler.h
    include/torrent/client/offset_handler.h
    include/torrent/client/metadata_handler.h
    include/torrent/client/group_handler.h
    include/torrent/client/admin_handler.h
    include/torrent/client/request_context.h
    include/torrent/admin/admin_server.h
    include/torrent/security/auth_manager.h
    include/torrent/security/acl_engine.h
    include/torrent/security/tls_manager.h
    include/torrent/security/sasl_provider.h
    include/torrent/security/audit_log.h
    include/torrent/metrics/metrics.h
    include/torrent/metrics/prometheus_exporter.h
    include/torrent/schema/schema_registry.h
    include/torrent/streams/topology.h
    include/torrent/connectors/connect.h
    include/torrent/proxy/proxy.h
    include/torrent/common/config.h
    include/torrent/common/thread_pool.h
    include/torrent/common/types.h
    include/torrent/common/rate_limiter.h
    include/torrent/common/iobuf.h
    include/torrent/common/circular_buffer.h
    include/torrent/common/lru_cache.h
    include/torrent/common/concurrent_map.h
)

# ---- Library target ----
if(TORRENT_BUILD_SHARED)
    add_library(torrent SHARED ${TORRENT_SOURCES} ${TORRENT_HEADERS})
else()
    add_library(torrent STATIC ${TORRENT_SOURCES} ${TORRENT_HEADERS})
endif()

target_include_directories(torrent
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(torrent
    PUBLIC
        nlohmann_json::nlohmann_json
        fmt::fmt
        spdlog::spdlog
        Threads::Threads
        OpenSSL::SSL
        OpenSSL::Crypto
        Boost::filesystem
        Boost::thread
    PRIVATE
        $<$<BOOL:${TORRENT_USE_GRPC}>:gRPC::grpc++>
        $<$<BOOL:${TORRENT_USE_ROCKSDB}>:RocksDB::rocksdb>
)

if(TORRENT_USE_PROMETHEUS)
    target_link_libraries(torrent PUBLIC prometheus-cpp::core prometheus-cpp::pull)
    target_compile_definitions(torrent PUBLIC TORRENT_HAS_PROMETHEUS=1)
endif()

if(TORRENT_USE_GRPC)
    target_compile_definitions(torrent PUBLIC TORRENT_HAS_GRPC=1)
endif()

if(TORRENT_USE_ROCKSDB)
    target_compile_definitions(torrent PUBLIC TORRENT_HAS_ROCKSDB=1)
endif()

# ---- Compiler options ----
target_compile_options(torrent PRIVATE
    $<$<CXX_COMPILER_ID:GNU>:-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion>
    $<$<CXX_COMPILER_ID:Clang>:-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion>
)

if(TORRENT_ENABLE_SANITIZERS)
    target_compile_options(torrent PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(torrent PRIVATE -fsanitize=address,undefined)
endif()

if(TORRENT_ENABLE_LTO)
    set_target_properties(torrent PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# ---- Tests ----
if(TORRENT_BUILD_TESTS)
    enable_testing()

    set(TEST_SOURCES
        tests/test_main.cpp
        tests/test_storage_segment.cpp
        tests/test_storage_log.cpp
        tests/test_storage_compaction.cpp
        tests/test_storage_recovery.cpp
        tests/test_network_protocol.cpp
        tests/test_network_transport.cpp
        tests/test_consensus_raft.cpp
        tests/test_consensus_election.cpp
        tests/test_consensus_snapshot.cpp
        tests/test_consensus_membership.cpp
        tests/test_broker_topic.cpp
        tests/test_broker_partition.cpp
        tests/test_broker_consumer_group.cpp
        tests/test_broker_producer.cpp
        tests/test_broker_fetch.cpp
        tests/test_broker_transaction.cpp
        tests/test_client_protocol.cpp
        tests/test_security_auth.cpp
        tests/test_security_acl.cpp
        tests/test_schema_registry.cpp
        tests/test_streams.cpp
        tests/test_connectors.cpp
        tests/test_integration.cpp
        tests/test_chaos.cpp
        tests/test_perf.cpp
    )

    add_executable(torrent_tests ${TEST_SOURCES})
    target_link_libraries(torrent_tests PRIVATE torrent GTest::gtest GTest::gtest_main)
    target_include_directories(torrent_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

    include(GoogleTest)
    gtest_discover_tests(torrent_tests)
endif()

# ---- Benchmarks ----
if(TORRENT_BUILD_BENCHMARKS)
    add_executable(torrent_benchmark
        benchmarks/bench_producer.cpp
        benchmarks/bench_consumer.cpp
        benchmarks/bench_storage.cpp
        benchmarks/bench_network.cpp
        benchmarks/bench_raft.cpp
        benchmarks/bench_end_to_end.cpp
    )
    target_link_libraries(torrent_benchmark PRIVATE torrent benchmark::benchmark)
    target_include_directories(torrent_benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
endif()

# ---- Executable ----
if(TORRENT_BUILD_CLI)
    add_executable(torrentd src/torrent/main.cpp)
    target_link_libraries(torrentd PRIVATE torrent)
endif()

# ---- Install ----
include(GNUInstallDirs)
install(TARGETS torrent
    EXPORT torrent-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
