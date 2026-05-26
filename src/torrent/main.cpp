#include "torrent/common/config.h"
#include "torrent/broker/server.h"
#include "torrent/common/signal_handler.h"
#include <spdlog/spdlog.h>
#include <iostream>
#include <csignal>

int main(int argc, char** argv) {
    // Set up logging
    spdlog::set_level(spdlog::level::info);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%n] %v");

    try {
        // Parse configuration
        torrent::config cfg = torrent::config::from_args(argc, argv);

        spdlog::info("torrent-mq v{}.{}.{} starting...",
                     torrent::kApiVersionMajor,
                     torrent::kApiVersionMinor,
                     torrent::kApiVersionPatch);

        spdlog::info("Broker ID: {}", cfg.broker_id());
        spdlog::info("Listening on port {} (plain), {} (tls)",
                     cfg.port(), cfg.tls_port());
        spdlog::info("Data directory: {}", cfg.log_dir());
        spdlog::info("Cluster ID: {}", cfg.cluster_id());

        // Create and start broker
        auto server = torrent::broker::create_server_from_file(std::to_string(cfg.broker_id()));

        // Set up signal handlers for graceful shutdown
        torrent::SignalHandler::register_handler(SIGINT, [&server]() {
            spdlog::info("Received SIGINT, shutting down...");
            server->shutdown();
        });

        torrent::SignalHandler::register_handler(SIGTERM, [&server]() {
            spdlog::info("Received SIGTERM, shutting down...");
            server->shutdown();
        });

        // Start the broker
        server->start();

        // Wait for shutdown signal
        torrent::SignalHandler::wait_for_signal();

        spdlog::info("torrent-mq shut down successfully");
        return 0;

    } catch (const std::exception& e) {
        spdlog::error("Fatal error: {}", e.what());
        return 1;
    }
}
