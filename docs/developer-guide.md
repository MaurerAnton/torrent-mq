# torrent-mq Developer Guide

## Codebase Structure

```
torrent-mq/
├── CMakeLists.txt              # Build system
├── README.md                   # Project overview
├── LICENSE                     # Business Source License 1.1
├── config/
│   └── torrent.yaml            # Example configuration
├── docs/
│   ├── architecture.md         # Architecture documentation
│   ├── api-reference.md        # API reference
│   └── deployment.md           # Deployment guide
├── include/
│   └── torrent/
│       ├── torrent.h           # Master include
│       ├── common/             # Common types and utilities
│       │   ├── types.h         # Fundamental types and constants
│       │   ├── config.h        # Configuration system
│       │   ├── thread_pool.h   # Priority-lane thread pool
│       │   ├── rate_limiter.h  # Token-bucket rate limiter
│       │   ├── iobuf.h         # Zero-copy I/O buffer
│       │   ├── circular_buffer.h # SPSC/MPMC queues
│       │   ├── lru_cache.h     # Thread-safe LRU cache
│       │   ├── concurrent_map.h # Thread-safe hash map
│       │   ├── endian.h        # Endian conversion + varint
│       │   ├── hostname.h      # Hostname/FQDN resolution
│       │   └── ...
│       ├── storage/            # Storage engine
│       │   ├── segment.h       # Append-only log segment
│       │   ├── log_manager.h   # Partition replica manager
│       │   ├── compaction.h    # Log compaction
│       │   └── types.h         # Storage data structures
│       ├── network/            # Network layer
│       │   ├── transport.h     # TCP/TLS transport
│       │   ├── connection.h    # Connection management
│       │   ├── protocol.h      # Wire protocol (60 API keys)
│       │   └── ...
│       ├── consensus/          # Raft consensus
│       │   ├── raft.h          # Raft node implementation
│       │   ├── raft_types.h    # Raft data types
│       │   └── ...
│       ├── broker/             # Broker core
│       │   ├── server.h        # Broker server
│       │   ├── topic_manager.h # Topic lifecycle
│       │   └── ...
│       ├── client/             # Client API handlers
│       │   ├── produce_handler.h
│       │   ├── fetch_handler.h
│       │   └── ...
│       ├── security/           # Authentication & authorization
│       ├── metrics/            # Observability
│       ├── schema/             # Schema registry
│       ├── streams/            # Stream processing
│       ├── connectors/         # Source/sink connectors
│       ├── proxy/              # Protocol proxies
│       └── admin/              # Admin REST API
├── src/
│   └── torrent/
│       ├── main.cpp            # Entry point
│       ├── common/             # Utility implementations
│       ├── storage/            # Storage implementations
│       ├── network/            # Network implementations
│       ├── consensus/          # Raft implementations
│       ├── broker/             # Broker implementations
│       ├── client/             # Client handler implementations
│       ├── security/           # Security implementations
│       ├── metrics/            # Metrics implementations
│       ├── schema/             # Schema implementations
│       ├── streams/            # Stream implementations
│       ├── connectors/         # Connector implementations
│       ├── proxy/              # Proxy implementations
│       ├── admin/              # Admin implementations
│       └── cli/                # CLI tool
├── tests/                      # Google Test test suites
│   ├── test_storage.cpp        # Storage tests (80+)
│   ├── test_network_consensus.cpp # Network + Raft tests (86)
│   ├── test_broker_client.cpp  # Broker + Client tests (215)
│   ├── test_security_schema.cpp # Security + Schema tests (100+)
│   └── test_integration.cpp    # Integration tests (50+)
├── benchmarks/                 # Google Benchmark suites
│   ├── bench_producer.cpp
│   └── bench_storage.cpp
└── third_party/                # Vendored dependencies (empty — FetchContent)
```

## Coding Conventions

### Naming

- **Classes/Structs**: PascalCase (`LogManager`, `RecordBatch`)
- **Functions/Methods**: snake_case (`append_batch()`, `find_position()`)
- **Variables**: snake_case (`base_offset`, `max_bytes`)
- **Member variables**: trailing underscore (`segments_`, `mutex_`)
- **Constants**: kPascalCase (`kDefaultPort`, `kInvalidOffset`)
- **Namespaces**: lowercase (`torrent::storage`, `torrent::raft`)
- **Enums**: enum class with snake_case values (`error_code::not_leader`)

### Header Guards

```cpp
#pragma once
```

### Includes

Order: project headers, third-party headers, standard library.

```cpp
#include "torrent/storage/segment.h"
#include "torrent/common/types.h"
#include <spdlog/spdlog.h>
#include <cstdint>
#include <vector>
```

### Error Handling

Use `result<T>` for fallible operations:

```cpp
result<offset_t> append(const RecordBatch& batch) {
    if (state_ != SegmentState::active) {
        return result<offset_t>::failure(
            error_code::invalid_request,
            "Segment is not active");
    }
    // ... operation ...
    return result<offset_t>::success(assigned_offset);
}
```

### Thread Safety

- `std::shared_mutex` for read-heavy data (multiple readers, single writer)
- `std::mutex` for write-heavy or simple synchronization
- `std::atomic` for counters and flags
- Document thread safety in method comments

### Logging

```cpp
#define STORAGE_LOG(logger, level, fmt, ...) \
    spdlog::get(logger)->level("storage: " fmt, ##__VA_ARGS__)

STORAGE_LOG("segment", info, "Segment {} opened, base_offset={}", id_, base_offset_);
```

## Building

### Prerequisites

```bash
# Install dependencies
sudo pacman -S base-devel cmake boost openssl grpc protobuf rocksdb
```

### Build Commands

```bash
# Debug build with tests
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DTORRENT_BUILD_TESTS=ON
make -j$(nproc)

# Release build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTORRENT_BUILD_TESTS=OFF
make -j$(nproc)

# With sanitizers
cmake .. -DCMAKE_BUILD_TYPE=Debug -DTORRENT_ENABLE_SANITIZERS=ON

# Run tests
./torrent_tests

# Run benchmarks
./torrent_benchmark
```

## Testing

### Running Tests

```bash
# All tests
cd build && ./torrent_tests

# Specific test suite
./torrent_tests --gtest_filter="SegmentTest.*"

# With verbose output
./torrent_tests --gtest_filter="SegmentTest.*" --gtest_print_time=1

# Run tests in parallel
./torrent_tests --gtest_parallel
```

### Writing Tests

```cpp
class MyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temp directory
        temp_dir_ = "/tmp/torrent_test_" + std::to_string(getpid());
        std::filesystem::create_directories(temp_dir_);
    }

    void TearDown() override {
        std::filesystem::remove_all(temp_dir_);
    }

    std::string temp_dir_;
};

TEST_F(MyTest, TestCaseName) {
    // Arrange
    auto config = create_test_config();

    // Act
    auto result = do_something(config);

    // Assert
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value, expected_value);
}
```

## Adding a New API

1. **Define API key constant** in `include/torrent/network/protocol.h`:
   ```cpp
   inline constexpr int16_t kMyNewApi = 70;
   ```

2. **Define request/response structs**:
   ```cpp
   struct MyNewRequest {
       std::string field1;
       int32_t field2;
   };
   struct MyNewResponse {
       error_code error{error_code::none};
       int64_t result;
   };
   ```

3. **Create handler** in `src/torrent/client/`:
   ```cpp
   class MyNewHandler {
   public:
       explicit MyNewHandler(BrokerServer& server);
       shared_buffer handle(const RequestContext& ctx, buffer_view body);
   };
   ```

4. **Register in request dispatcher** (`server.cpp`):
   ```cpp
   dispatcher_->register_handler(kMyNewApi, my_new_handler);
   ```

5. **Add test cases** in appropriate test file.

## Protocol Buffer Usage

For gRPC services:

```protobuf
// proto/torrent_raft.proto
service RaftService {
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
    rpc InstallSnapshot(stream InstallSnapshotRequest) returns (InstallSnapshotResponse);
}
```

## Performance Profiling

### Perf

```bash
perf record -g ./torrentd --config-file test_config.yaml
perf report
```

### Valgrind (memory)

```bash
valgrind --leak-check=full --show-leak-kinds=all ./torrent_tests
```

### strace (system calls)

```bash
strace -c -p $(pidof torrentd)  # Running process
strace -e trace=network ./torrentd  # Network syscalls only
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass: `./torrent_tests`
5. Ensure code compiles with `-Wall -Wextra -Wpedantic`
6. Submit a pull request

### Commit Messages

```
subsystem: brief description

Detailed explanation of what was changed and why.
Include any relevant issue numbers.

Fixes: #123
```

### Pull Request Checklist

- [ ] Code compiles on GCC 13+ and Clang 16+
- [ ] All existing tests pass
- [ ] New tests added for new functionality
- [ ] Thread safety documented
- [ ] Error handling for all edge cases
- [ ] Performance not degraded (benchmarks pass)
- [ ] Documentation updated

## Dependencies

### Core (Required)
- C++20 compiler (GCC 13+, Clang 16+)
- CMake 3.20+
- Boost 1.91+ (filesystem, thread)
- OpenSSL 3.0+
- spdlog (fetched via CMake)
- nlohmann/json (fetched via CMake)
- fmt (fetched via CMake)

### Optional
- gRPC + Protobuf (for gRPC transport)
- RocksDB (for persistent metadata)
- prometheus-cpp (for metrics export)
- Google Test (for unit tests)
- Google Benchmark (for benchmarks)

All optional dependencies are fetched via CMake FetchContent or can be
provided by the system package manager.
