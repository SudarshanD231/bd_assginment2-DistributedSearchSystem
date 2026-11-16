#!/usr/bin/env python3
# Node 1: ingest one phase file into Iceberg. Idempotent.
# Tables: lake.db.pages, lake.db.raw_links (declared links), lake.db.links (valid links only)
# Usage: spark-submit ingest.py --phase 0 --input hdfs:///input/phase0/pages.jsonl
import argparse
import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

DB = "lake.db"
WAREHOUSE = "hdfs://node2:9000/warehouse"
CKPT = "hdfs://node2:9000/tmp/spark-ckpt"

ID_KEYS = ("doc_id", "id", "docid", "page_id", "pageid")
URL_KEYS = ("url", "link", "href")
TITLE_KEYS = ("title", "name")
TEXT_KEYS = ("text", "content", "body")
LINK_KEYS = ("links", "outlinks", "out_links")
VER_KEYS = ("version", "ver", "revision", "rev", "seq")
OP_KEYS = ("op", "operation", "action", "change", "change_type")
DEL_OPS = ("delete", "deleted", "remove", "removed", "del", "tombstone")
DEL_FLAGS = ("deleted", "is_deleted", "tombstone")

ROW = T.StructType([
    T.StructField("line_no", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("doc_id", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("links", T.ArrayType(T.StringType())),
    T.StructField("version", T.LongType()),
    T.StructField("is_delete", T.BooleanType()),
])


def log(msg):
    print(">> " + str(msg), flush=True)


def get_spark(app):
    spark = (SparkSession.builder.appName(app)
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lake.type", "hadoop")
             .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(CKPT)
    return spark


def to_version(v):
    if v is None or isinstance(v, bool):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    try:
        return int(float(s))
    except ValueError:
        pass
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def parse_line(pair):
    line, idx = pair
    bad = (idx, "", None, None, None, None, None, 0, False)
    s = line.strip()
    if not s:
        return (idx, "blank") + bad[2:]
    try:
        o = json.loads(s)
    except Exception:
        return (idx, "bad_json") + bad[2:]
    if not isinstance(o, dict):
        return (idx, "bad_json") + bad[2:]

    def pick(keys):
        for k in keys:
            if k in o and o[k] is not None:
                return o[k]
        return None

    doc_id = pick(ID_KEYS)
    doc_id = str(doc_id).strip() if doc_id is not None else ""
    if not doc_id:
        return (idx, "missing_id") + bad[2:]
    op = str(pick(OP_KEYS) or "").strip().lower()
    is_delete = op in DEL_OPS or any(o.get(k) is True for k in DEL_FLAGS)
    version = to_version(pick(VER_KEYS))
    if is_delete:
        return (idx, "ok", doc_id, None, None, None, [], version, True)
    url = pick(URL_KEYS)
    text = pick(TEXT_KEYS)
    if not isinstance(url, str) or not url.strip() or not isinstance(text, str) or not text.strip():
        return (idx, "missing_field", doc_id, None, None, None, None, version, False)
    title = pick(TITLE_KEYS)
    title = str(title) if title is not None else ""
    raw = pick(LINK_KEYS)
    links = []
    if isinstance(raw, list):
        for x in raw:
            if isinstance(x, dict):
                x = x.get("doc_id") or x.get("id") or x.get("target")
            if x is not None and str(x).strip():
                links.append(str(x).strip())
    return (idx, "ok", doc_id, url.strip(), title, text, links, version, False)


def parse_input(spark, path):
    rdd = spark.sparkContext.textFile(path).zipWithIndex().map(parse_line)
    return spark.createDataFrame(rdd, ROW)


def dedupe(valid):
    # highest version wins; tie -> the record that appears later in the file
    w = Window.partitionBy("doc_id").orderBy(F.col("version").desc(), F.col("line_no").desc())
    return valid.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def with_hash(df):
    links_key = F.array_join(F.array_sort(F.array_distinct(F.coalesce("links", F.array()))), ",")
    body = F.concat_ws("\u0001", F.coalesce("url", F.lit("")), F.coalesce("title", F.lit("")),
                       F.coalesce("text", F.lit("")), links_key)
    return df.withColumn("content_hash", F.sha2(body, 256))


def plan_changes(inc, cur):
    # inc: deduped incoming; cur: (doc_id, cur_version, cur_hash) from lake.db.pages
    j = inc.join(cur, "doc_id", "left")
    exists = F.col("cur_version").isNotNull()
    newer = F.col("version") > F.col("cur_version")
    same_ver_changed = (F.col("version") == F.col("cur_version")) & (F.col("content_hash") != F.col("cur_hash"))
    action = (F.when(F.col("is_delete") & exists & (F.col("version") >= F.col("cur_version")), "D")
              .when(F.col("is_delete") & exists, "stale")
              .when(F.col("is_delete"), "delete_missing")
              .when(~exists, "I")
              .when(newer | same_ver_changed, "U")
              .when(F.col("version") < F.col("cur_version"), "stale")
              .otherwise("unchanged"))
    return j.withColumn("action", action)


def new_raw_links(cur_raw, planned):
    # For every incoming doc that is (now) the current version, its declared links are the incoming
    # links. Recomputing them even for "unchanged" docs makes a re-run after a crash self-healing,
    # and an identical re-run produces identical rows (so nothing is written).
    owned = planned.filter(F.col("action").isin("I", "U", "D", "unchanged"))
    owned_ids = owned.select(F.col("doc_id").alias("src")).distinct()
    kept = cur_raw.join(owned_ids, "src", "left_anti")
    added = (owned.filter(F.col("action").isin("I", "U", "unchanged"))
             .select(F.col("doc_id").alias("src"), F.explode("links").alias("dst")))
    return kept.unionByName(added).filter(F.col("dst") != "").distinct()


def valid_links(raw, pages_ids):
    ids = pages_ids.select("doc_id")
    s = raw.join(ids.withColumnRenamed("doc_id", "src"), "src")
    return s.join(ids.withColumnRenamed("doc_id", "dst"), "dst").select("src", "dst").distinct()


def same_rows(a, b):
    return a.exceptAll(b).limit(1).count() == 0 and b.exceptAll(a).limit(1).count() == 0


# ---------- Iceberg I/O (kept small so the logic above can be tested on its own) ----------
def ensure_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
    spark.sql("CREATE TABLE IF NOT EXISTS lake.db.pages (doc_id STRING, url STRING, title STRING, "
              "text STRING, version BIGINT, content_hash STRING, phase INT) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS lake.db.raw_links (src STRING, dst STRING) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS lake.db.links (src STRING, dst STRING) USING iceberg")


def read_table(spark, name):
    return spark.table(DB + "." + name)


def snapshot_count(spark, name):
    return spark.sql("SELECT snapshot_id FROM " + DB + "." + name + ".snapshots").count()


def merge_pages(spark, chg):
    chg.createOrReplaceTempView("chg")
    spark.sql("""
        MERGE INTO lake.db.pages t
        USING chg s
        ON t.doc_id = s.doc_id
        WHEN MATCHED AND s.action = 'D' THEN DELETE
        WHEN MATCHED AND s.action = 'U' THEN UPDATE SET
            t.url = s.url, t.title = s.title, t.text = s.text, t.version = s.version,
            t.content_hash = s.content_hash, t.phase = s.phase
        WHEN NOT MATCHED AND s.action = 'I' THEN INSERT
            (doc_id, url, title, text, version, content_hash, phase)
            VALUES (s.doc_id, s.url, s.title, s.text, s.version, s.content_hash, s.phase)
    """)


def overwrite_table(spark, df, name):
    # static INSERT OVERWRITE on an unpartitioned Iceberg table replaces the whole table atomically
    df.createOrReplaceTempView("new_rows")
    spark.sql("INSERT OVERWRITE " + DB + "." + name + " SELECT src, dst FROM new_rows")


def show_snapshots(spark):
    for name in ("pages", "raw_links", "links"):
        log("snapshots of lake.db." + name + ":")
        (spark.sql("SELECT committed_at, snapshot_id, operation, summary['added-records'] AS added, "
                   "summary['deleted-records'] AS deleted, summary['total-records'] AS total FROM "
                   + DB + "." + name + ".snapshots ORDER BY committed_at")
         .show(50, truncate=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, required=True)
    ap.add_argument("--input", required=True)
    a = ap.parse_args()
    spark = get_spark("ingest-phase-%d" % a.phase)
    ensure_tables(spark)
    before = {n: snapshot_count(spark, n) for n in ("pages", "raw_links", "links")}

    # 1. parse + validate
    parsed = parse_input(spark, a.input).checkpoint(eager=True)
    stats = {r["status"]: r["count"] for r in parsed.groupBy("status").count().collect()}
    total = sum(stats.values())
    valid = parsed.filter("status = 'ok'")
    n_valid = stats.get("ok", 0)
    log("VALIDATION phase=%d file=%s" % (a.phase, a.input))
    log("  lines total        : %d" % total)
    for k in sorted(stats):
        if k != "ok":
            log("  rejected %-10s: %d" % (k, stats[k]))
    log("  valid records      : %d" % n_valid)

    # 2. dedupe by doc_id
    inc = with_hash(dedupe(valid)).checkpoint(eager=True)
    n_unique = inc.count()
    n_tomb = inc.filter("is_delete").count()
    log("  duplicates removed : %d" % (n_valid - n_unique))
    log("  unique doc_ids     : %d  (tombstones: %d)" % (n_unique, n_tomb))

    # 3. decide what changes against the CURRENT Iceberg state, and materialize it BEFORE any write
    cur = read_table(spark, "pages").select("doc_id", F.col("version").alias("cur_version"),
                                             F.col("content_hash").alias("cur_hash"))
    planned = plan_changes(inc, cur).withColumn("phase", F.lit(a.phase))
    planned = planned.checkpoint(eager=True)
    acts = {r["action"]: r["count"] for r in planned.groupBy("action").count().collect()}
    changes = planned.filter(F.col("action").isin("I", "U", "D")).checkpoint(eager=True)
    n_chg = changes.count()
    log("CHANGES: inserted=%d updated=%d deleted=%d unchanged=%d stale(older version)=%d delete-of-missing=%d"
        % (acts.get("I", 0), acts.get("U", 0), acts.get("D", 0), acts.get("unchanged", 0),
           acts.get("stale", 0), acts.get("delete_missing", 0)))

    # 4. MERGE pages (only if something changed -> no empty snapshot on a re-run)
    if n_chg > 0:
        merge_pages(spark, changes.select("doc_id", "url", "title", "text", "version",
                                          "content_hash", "phase", "action"))
        log("pages: MERGE committed")
    else:
        log("pages: nothing to merge (idempotent re-run)")

    # 5. declared links (all links as written in the input, even to pages that do not exist yet)
    cur_raw = read_table(spark, "raw_links").select("src", "dst")
    raw = new_raw_links(cur_raw, planned).checkpoint(eager=True)
    if same_rows(raw, cur_raw):
        log("raw_links: unchanged")
    else:
        overwrite_table(spark, raw, "raw_links")
        log("raw_links: rewritten (%d declared links)" % raw.count())

    # 6. valid links = declared links whose src AND dst pages exist (drops dangling + links of deleted docs)
    pages_now = read_table(spark, "pages").select("doc_id").checkpoint(eager=True)
    links = valid_links(raw, pages_now).checkpoint(eager=True)
    n_raw = raw.count()
    n_links = links.count()
    log("links: declared=%d valid=%d dangling/removed=%d" % (n_raw, n_links, n_raw - n_links))
    cur_links = read_table(spark, "links").select("src", "dst")
    if same_rows(links, cur_links):
        log("links: unchanged")
    else:
        overwrite_table(spark, links, "links")
        log("links: rewritten")

    after = {n: snapshot_count(spark, n) for n in ("pages", "raw_links", "links")}
    show_snapshots(spark)
    n_pages = read_table(spark, "pages").count()
    n_links_tbl = read_table(spark, "links").count()
    log("FINAL COUNTS: pages=%d links=%d" % (n_pages, n_links_tbl))
    log("NEW SNAPSHOTS this run: pages=%d raw_links=%d links=%d"
        % (after["pages"] - before["pages"], after["raw_links"] - before["raw_links"],
           after["links"] - before["links"]))
    spark.stop()


main()
