"""Publish the sample Debezium events to Kafka, keyed by policy_id like Debezium does.

python scripts/produce_events.py --bootstrap localhost:19092
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from confluent_kafka import Producer


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--topic", default="dbserver.public.policies")
    p.add_argument(
        "files", nargs="*", default=["data/cdc/snapshot.jsonl", "data/cdc/updates.jsonl"]
    )
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})
    sent = 0
    for path in args.files:
        for line in Path(path).read_text().splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)["payload"]
            image = payload.get("after") or payload.get("before") or {}
            # Same key -> same partition -> per-policy ordering is preserved.
            producer.produce(args.topic, key=image.get("policy_id", ""), value=line)
            sent += 1
    producer.flush(10)
    print(f"published {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
