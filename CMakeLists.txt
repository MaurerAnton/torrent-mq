cmake_minimum_required(VERSION 3.20)
project(torrent-mq VERSION 0.1.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(FetchContent)
FetchContent_Declare(nlohmann_json GIT_REPOSITORY https://github.com/nlohmann/json.git GIT_TAG v3.11.3)
FetchContent_MakeAvailable(nlohmann_json)
FetchContent_Declare(fmt GIT_REPOSITORY https://github.com/fmtlib/fmt.git GIT_TAG 11.0.2)
FetchContent_MakeAvailable(fmt)
FetchContent_Declare(spdlog GIT_REPOSITORY https://github.com/gabime/spdlog.git GIT_TAG v1.15.1)
FetchContent_MakeAvailable(spdlog)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS filesystem thread)

set(TORRENT_SOURCES
src/torrent/cli/torrentctl.cpp
    src/torrent/client/admin_client.cpp
    src/torrent/client/consumer_client.cpp
    src/torrent/common/config.cpp
    src/torrent/common/endian.cpp
    src/torrent/common/hostname.cpp
    src/torrent/common/iobuf.cpp
    src/torrent/common/pidfile.cpp
    src/torrent/common/rate_limiter.cpp
    src/torrent/common/retry.cpp
    src/torrent/common/signal_handler.cpp
    src/torrent/common/thread_pool.cpp
    src/torrent/connectors/connect_framework.cpp
    src/torrent/connectors/debezium_source.cpp
    src/torrent/connectors/elasticsearch_sink.cpp
    src/torrent/connectors/influxdb_sink.cpp
    src/torrent/connectors/jdbc_sink.cpp
    src/torrent/connectors/kafka_mirror.cpp
    src/torrent/connectors/mongodb_source.cpp
    src/torrent/connectors/offset_backing_store.cpp
    src/torrent/connectors/redis_sink.cpp
    src/torrent/connectors/s3_sink.cpp
    src/torrent/connectors/sink_connector.cpp
    src/torrent/connectors/source_connector.cpp
    src/torrent/connectors/task_manager.cpp
    src/torrent/connectors/transforms.cpp
    src/torrent/consensus/metadata_manager.cpp
    src/torrent/consensus/raft_election.cpp
    src/torrent/consensus/raft_heartbeat.cpp
    src/torrent/consensus/raft_log.cpp
    src/torrent/consensus/raft_membership.cpp
    src/torrent/consensus/raft_node.cpp
    src/torrent/consensus/raft_rpc.cpp
    src/torrent/consensus/raft_snapshot.cpp
    src/torrent/main.cpp
    src/torrent/metrics/broker_metrics.cpp
    src/torrent/metrics/metrics.cpp
    src/torrent/metrics/metrics_collector.cpp
    src/torrent/metrics/metrics_engine.cpp
    src/torrent/metrics/prometheus_exporter.cpp
    src/torrent/network/acceptor.cpp
    src/torrent/network/connection.cpp
    src/torrent/network/connection_limiter.cpp
    src/torrent/network/connection_pool.cpp
    src/torrent/network/flow_control.cpp
    src/torrent/network/grpc_server.cpp
    src/torrent/network/message_codec.cpp
    src/torrent/network/protocol_serializer.cpp
    src/torrent/network/tcp_transport.cpp
    src/torrent/proxy/amqp_bridge.cpp
    src/torrent/proxy/http_bridge.cpp
    src/torrent/proxy/mqtt_bridge.cpp
    src/torrent/proxy/nats_bridge.cpp
    src/torrent/proxy/rest_proxy.cpp
    src/torrent/proxy/websocket_proxy.cpp
    src/torrent/schema/avro_provider.cpp
    src/torrent/schema/compatibility_checker.cpp
    src/torrent/schema/json_schema_provider.cpp
    src/torrent/schema/protobuf_provider.cpp
    src/torrent/schema/schema_registry.cpp
    src/torrent/schema/serde.cpp
    src/torrent/security/audit_log.cpp
    src/torrent/security/auth_manager.cpp
    src/torrent/security/tls_manager.cpp
    src/torrent/storage/archival.cpp
    src/torrent/storage/batch_cache.cpp
    src/torrent/storage/cleaner.cpp
    src/torrent/storage/compaction.cpp
    src/torrent/storage/disk_io.cpp
    src/torrent/storage/encryption.cpp
    src/torrent/storage/file_store.cpp
    src/torrent/storage/log_manager.cpp
    src/torrent/storage/offset_index.cpp
    src/torrent/storage/page_cache.cpp
    src/torrent/storage/recovery.cpp
    src/torrent/storage/rocksdb_engine.cpp
    src/torrent/storage/rocksdb_store.cpp
    src/torrent/storage/segment.cpp
    src/torrent/storage/segment_index.cpp
    src/torrent/storage/snapshot.cpp
    src/torrent/storage/tiered_storage.cpp
    src/torrent/storage/time_index.cpp
    src/torrent/streams/aggregate_processor.cpp
    src/torrent/streams/filter_processor.cpp
    src/torrent/streams/grouped_processor.cpp
    src/torrent/streams/join_processor.cpp
    src/torrent/streams/punctuator.cpp
    src/torrent/streams/repartition.cpp
    src/torrent/streams/state_store.cpp
    src/torrent/streams/stream_processor.cpp
    src/torrent/streams/table_processor.cpp
    src/torrent/streams/topology_builder.cpp
    src/torrent/streams/window_processor.cpp
)

add_library(torrent STATIC ${TORRENT_SOURCES})
target_include_directories(torrent PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include> PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(torrent PUBLIC nlohmann_json::nlohmann_json fmt::fmt spdlog::spdlog Threads::Threads OpenSSL::SSL OpenSSL::Crypto Boost::filesystem Boost::thread)
