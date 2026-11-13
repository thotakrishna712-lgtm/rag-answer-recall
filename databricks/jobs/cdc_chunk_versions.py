"""Databricks job: Debezium topic -> SCD2 chunk_versions + answer recalls.

Deployed by databricks/databricks.yml. Parameters:
  --catalog --schema --bootstrap --topic --checkpoint
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from ragrecall.lakehouse import Tables, foreach_batch


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", default="policy_rag")
    p.add_argument("--bootstrap", required=True)
    p.add_argument("--topic", default="dbserver.public.policies")
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--available-now", action="store_true", help="drain and stop (scheduled)")
    args = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    tables = Tables.in_schema(args.catalog, args.schema)

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
        .where("value IS NOT NULL")  # Debezium tombstones
    )
    writer = (
        stream.writeStream.foreachBatch(foreach_batch(tables))
        .option("checkpointLocation", args.checkpoint)
        .queryName("ragrecall_cdc_chunk_versions")
    )
    writer = writer.trigger(availableNow=True) if args.available_now else writer
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
