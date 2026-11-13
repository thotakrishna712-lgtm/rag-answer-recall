"""Confluent Kafka consumer for Debezium topics.

Offsets are committed only after the event is applied (at-least-once delivery), and
apply is idempotent on source LSN, so redelivery after a crash is a no-op:
effectively-once results without Kafka transactions.

    python -m ragrecall.adapters.kafka_consumer --bootstrap localhost:9092 \
        --topic dbserver.public.policies --db var/ragrecall.db
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any, Protocol

from ragrecall.service import RecallService, build_service

log = logging.getLogger("ragrecall.kafka")


class MessageLike(Protocol):
    """The subset of confluent_kafka.Message the consumer loop uses."""

    def error(self) -> Any:
        """Broker or partition error, or None."""

    def value(self) -> bytes | None:
        """Message payload; None for a Debezium tombstone."""

    def offset(self) -> int:
        """Partition offset, used for logging."""


class ConsumerLike(Protocol):
    """The subset of confluent_kafka.Consumer the loop uses (lets tests pass a fake)."""

    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to the CDC topic."""

    def poll(self, timeout: float) -> MessageLike | None:
        """Next message, or None when the poll times out."""

    def commit(self, message: MessageLike, asynchronous: bool) -> Any:
        """Commit the offset of an applied message."""

    def close(self) -> None:
        """Leave the group and release resources."""


def consume(
    consumer: ConsumerLike,
    service: RecallService,
    topic: str,
    max_messages: int | None = None,
    max_idle_polls: int | None = None,
    poll_timeout: float = 1.0,
) -> dict[str, int]:
    stats = {"processed": 0, "skipped_tombstones": 0, "errors": 0}
    consumer.subscribe([topic])
    idle = 0
    try:
        while max_messages is None or stats["processed"] < max_messages:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                idle += 1
                if max_idle_polls is not None and idle >= max_idle_polls:
                    break
                continue
            idle = 0
            if msg.error():
                stats["errors"] += 1
                log.warning("kafka error: %s", msg.error())
                continue
            value = msg.value()
            if value is None:  # Debezium tombstone after a delete; compaction marker only
                stats["skipped_tombstones"] += 1
                consumer.commit(message=msg, asynchronous=False)
                continue
            result = service.apply(json.loads(value))
            log.info("offset=%s lsn=%s %s", msg.offset(), result.lsn, result.status)
            consumer.commit(message=msg, asynchronous=False)
            stats["processed"] += 1
    finally:
        consumer.close()
    return stats


def main() -> None:
    from confluent_kafka import Consumer

    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="dbserver.public.policies")
    p.add_argument("--group", default="ragrecall-indexer")
    p.add_argument("--db", default="var/ragrecall.db")
    p.add_argument("--entitlements", default="data/entitlements.json")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consume(consumer, build_service(args.db, args.entitlements), args.topic)


if __name__ == "__main__":
    main()
