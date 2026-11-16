#!/usr/bin/env python3
# Node 1: Iceberg snapshot / time-travel demo on lake.db.pages.
# Compares the FIRST snapshot (phase 0) with the CURRENT one. Usage: spark-submit timetravel.py [--doc 94]
import argparse
from pyspark.sql import SparkSession

WAREHOUSE = "hdfs://node2:9000/warehouse"


def get_spark():
    spark = (SparkSession.builder.appName("timetravel")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lake.type", "hadoop")
             .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc", default="94")
    a = ap.parse_args()
    spark = get_spark()
    print(">> SNAPSHOT HISTORY of lake.db.pages")
    spark.sql("SELECT committed_at, snapshot_id, operation, summary['total-records'] AS total_records "
              "FROM lake.db.pages.snapshots ORDER BY committed_at").show(truncate=False)
    snaps = spark.sql("SELECT snapshot_id, CAST(committed_at AS STRING) AS committed_at FROM lake.db.pages.snapshots "
                      "ORDER BY committed_at").collect()
    old_id = snaps[0]["snapshot_id"]
    old_ts = snaps[0]["committed_at"]
    new_id = snaps[-1]["snapshot_id"]
    old_n = spark.sql("SELECT count(1) FROM lake.db.pages VERSION AS OF %d" % old_id).first()[0]
    new_n = spark.sql("SELECT count(1) FROM lake.db.pages").first()[0]
    ts_n = spark.sql("SELECT count(1) FROM lake.db.pages TIMESTAMP AS OF '%s'" % old_ts).first()[0]
    print(">> pages VERSION AS OF %d (first snapshot): %d rows" % (old_id, old_n))
    print(">> pages TIMESTAMP AS OF '%s'          : %d rows" % (old_ts, ts_n))
    print(">> pages now (snapshot %d)            : %d rows" % (new_id, new_n))
    old = spark.sql("SELECT doc_id, content_hash FROM lake.db.pages VERSION AS OF %d" % old_id)
    new = spark.sql("SELECT doc_id, content_hash FROM lake.db.pages")
    deleted = old.join(new, "doc_id", "left_anti").count()
    inserted = new.join(old, "doc_id", "left_anti").count()
    updated = (old.withColumnRenamed("content_hash", "h_old").join(new, "doc_id")
               .filter("h_old <> content_hash").count())
    print(">> DIFF first -> now: inserted=%d updated=%d deleted=%d" % (inserted, updated, deleted))
    q = "SELECT doc_id, title, phase, substr(text, 1, 60) AS text_start FROM lake.db.pages %s WHERE doc_id = '%s'"
    print(">> doc %s in the FIRST snapshot:" % a.doc)
    spark.sql(q % ("VERSION AS OF %d" % old_id, a.doc)).show(truncate=False)
    print(">> doc %s NOW:" % a.doc)
    spark.sql(q % ("", a.doc)).show(truncate=False)
    spark.stop()


main()
