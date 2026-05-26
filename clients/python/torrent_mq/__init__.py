"""
torrent-mq — Python Client SDK

A high-performance Python client for torrent-mq, a distributed messaging
and streaming platform. Provides producer, consumer, and admin clients
with full support for batching, compression, consumer groups, and
administrative operations.

Quick Start
-----------
Producer::

    from torrent_mq import TorrentProducer

    producer = TorrentProducer(bootstrap_servers="localhost:9092")
    metadata = producer.send("my-topic", b"hello torrent-mq")
    producer.close()

Consumer::

    from torrent_mq import TorrentConsumer

    consumer = TorrentConsumer(
        bootstrap_servers="localhost:9092",
        group_id="my-group",
    )
    consumer.subscribe(["my-topic"])
    while True:
        for record in consumer.poll(1000):
            print(f"{record.topic}/{record.partition} @ {record.offset}: {record.value}")
    consumer.close()

Admin::

    from torrent_mq import TorrentAdmin

    admin = TorrentAdmin(bootstrap_servers="localhost:9092")
    admin.create_topic("my-topic", partitions=3, replication_factor=3)
    topics = admin.list_topics()
    admin.close()

Package Contents
----------------
- TorrentProducer: Batching, async message producer.
- TorrentConsumer: Consumer group member with rebalance support.
- TorrentAdmin: Administrative client for topic/group/config management.
- ConsumerRecord: A single consumed record.
- RecordMetadata: Metadata for a produced record.
- RebalanceListener: Callback interface for group rebalance events.
- ConfigEntry, TopicDescription, PartitionInfo, BrokerInfo, etc.: DTOs.
- protocol module: Low-level wire format (advanced use).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

__version__ = "0.1.0"
__title__ = "torrent-mq"
__description__ = "Python client SDK for torrent-mq distributed messaging"
__author__ = "Nous Research"
__license__ = "MIT"

# ---------------------------------------------------------------------------
# Public API — Producer
# ---------------------------------------------------------------------------

from .producer import (
    TorrentProducer,
    RecordMetadata,
    ProducerConfig,
    CompressionType,
)

# ---------------------------------------------------------------------------
# Public API — Consumer
# ---------------------------------------------------------------------------

from .consumer import (
    TorrentConsumer,
    ConsumerRecord,
    ConsumerConfig,
    RebalanceListener,
    OffsetReset,
    Assignor,
)

# ---------------------------------------------------------------------------
# Public API — Admin
# ---------------------------------------------------------------------------

from .admin import (
    TorrentAdmin,
    ConfigEntry,
    TopicDescription,
    PartitionInfo,
    BrokerInfo,
    ClusterDescription,
    ConsumerGroupDescription,
    ConsumerGroupSummary,
    ConsumerGroupMember,
)

# ---------------------------------------------------------------------------
# Protocol (advanced use)
# ---------------------------------------------------------------------------

from . import protocol

# ---------------------------------------------------------------------------
# All exports
# ---------------------------------------------------------------------------

__all__ = [
    # Version
    "__version__",
    # Producer
    "TorrentProducer",
    "RecordMetadata",
    "ProducerConfig",
    "CompressionType",
    # Consumer
    "TorrentConsumer",
    "ConsumerRecord",
    "ConsumerConfig",
    "RebalanceListener",
    "OffsetReset",
    "Assignor",
    # Admin
    "TorrentAdmin",
    "ConfigEntry",
    "TopicDescription",
    "PartitionInfo",
    "BrokerInfo",
    "ClusterDescription",
    "ConsumerGroupDescription",
    "ConsumerGroupSummary",
    "ConsumerGroupMember",
    # Protocol
    "protocol",
]
