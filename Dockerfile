# ---- Build Stage ----
FROM docker.io/library/gcc:13 AS builder

RUN apt-get update && apt-get install -y \
    cmake \
    libboost-filesystem-dev \
    libboost-thread-dev \
    libssl-dev \
    libgrpc++-dev \
    libprotobuf-dev \
    librocksdb-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN mkdir build && cd build && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DTORRENT_USE_GRPC=ON \
        -DTORRENT_USE_ROCKSDB=ON \
        -DTORRENT_USE_PROMETHEUS=ON \
        -DTORRENT_BUILD_TESTS=OFF \
        -DTORRENT_BUILD_CLI=ON \
        -DTORRENT_BUILD_SHARED=OFF \
    && make -j$(nproc) torrentd torrentctl \
    && strip torrentd torrentctl

# ---- Runtime Stage ----
FROM docker.io/library/debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    libboost-filesystem1.83.0 \
    libboost-thread1.83.0 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -s /bin/false -u 1000 torrent && \
    mkdir -p /var/lib/torrent/data /etc/torrent && \
    chown -R torrent:torrent /var/lib/torrent /etc/torrent

COPY --from=builder /build/build/torrentd /usr/local/bin/
COPY --from=builder /build/build/torrentctl /usr/local/bin/
COPY config/torrent.yaml /etc/torrent/config.yaml.example

USER torrent
EXPOSE 9092 9093 9644 9090

VOLUME ["/var/lib/torrent/data"]

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD torrentctl cluster health || exit 1

ENTRYPOINT ["/usr/local/bin/torrentd"]
CMD ["--config-file", "/etc/torrent/config.yaml"]
