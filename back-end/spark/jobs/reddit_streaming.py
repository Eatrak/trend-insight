import json
import os
import re
import sqlite3
import shutil
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# =============================================================================
# 1. SETTINGS
# =============================================================================
KAFKA_URL         = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL      = "/data/topics.db"
CHECKPOINT_DIR    = "/checkpoints"
DAILY_METRICS_OUT = "reddit.topic.metrics"
POST_SCHEMA       = "event_id STRING, created_utc STRING, text STRING, score INT, num_comments INT"

# =============================================================================
# 2. INTELLIGENT SCANNER (Stateful UDF)
# =============================================================================

# Global cache for workers
_TOPIC_CACHE = {"regex": None, "lookup": {}, "last_updated": 0}

def get_topics_cached():
    """Worker-side caching: Reloads DB only every 60 seconds."""
    now = time.time()
    if now - _TOPIC_CACHE["last_updated"] > 60:
        if os.path.exists(DATABASE_URL):
            try: shutil.copyfile(DATABASE_URL, "/tmp/topics_worker.db")
            except: pass
            
            try:
                lookup = {} # term -> [topic_ids]
                all_terms = set()
                
                with sqlite3.connect("/tmp/topics_worker.db") as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute("SELECT id, keywords FROM topics WHERE is_active = 1").fetchall()
                    for r in rows:
                        raw = (r["keywords"] or "").strip()
                        kws = json.loads(raw) if raw.startswith("[") else [x.strip().lower() for x in raw.split(",") if x]
                        for k in kws:
                            if k not in lookup: lookup[k] = []
                            lookup[k].append(r["id"])
                            all_terms.add(k)
                
                # Verify we have terms before compiling
                if all_terms:
                    # Sort by length desc to match longest terms first (e.g. "new york" before "new")
                    sorted_terms = sorted(list(all_terms), key=len, reverse=True)
                    pattern = "|".join([re.escape(t) for t in sorted_terms])
                    _TOPIC_CACHE["regex"] = re.compile(f"(?i)({pattern})")
                else:
                    _TOPIC_CACHE["regex"] = None
                    
                _TOPIC_CACHE["lookup"] = lookup
                _TOPIC_CACHE["last_updated"] = now
            except: pass
            
    return _TOPIC_CACHE

def main():
    spark = SparkSession.builder.appName("SimpleStreamer").getOrCreate()

    @F.udf(returnType="array<struct<topic_id:string, term:string>>")
    def scan_text(text):
        """Scans text using a single compiled RegeX (O(1) pass)."""
        if not text: return []
        
        cache = get_topics_cached()
        regex = cache["regex"]
        lookup = cache["lookup"]
        
        if not regex: return []

        matches = []
        # Single Pass Scan
        found_terms = set(regex.findall(text))
        for term in found_terms:
            lower_term = term.lower()
            if lower_term in lookup:
                for tid in lookup[lower_term]:
                    matches.append((tid, lower_term))
                    
        return list(set(matches))
    
    # 1. READ
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", "reddit.raw.posts,reddit.raw.comments")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("data"))
        .select("data.*")
    )

    # 2. MATCH & AGGREGATE
    metrics_stream = (
        raw_stream
        # Transformation: Tag matching topics
        .withColumn("matches", scan_text(F.col("text")))
        .filter("size(matches) > 0")
        .selectExpr("*", "explode(matches) as m")
        .selectExpr("*", "m.topic_id", "m.term")
        .drop("matches", "m")
        
        # Aggregation: Count per day
        .withColumn("time", F.col("created_utc").cast("timestamp"))
        .withWatermark("time", "35 days")
        .dropDuplicates(["event_id", "topic_id"])
        .groupBy("topic_id", F.window("time", "1 day").alias("day"))
        .agg(
            F.count("*").alias("mentions"),
            F.sum(F.col("score") + F.coalesce(F.col("num_comments"), F.lit(0))).alias("engagement")
        )
        .selectExpr(
            "topic_id", "mentions", "engagement",
            "day.start as start", "day.end as end",
            "'1d' as window_type"
        )
    )

    # 3. WRITE
    (metrics_stream.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("topic", DAILY_METRICS_OUT)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/metrics_simple_v3")
        .outputMode("update")
        .start()
        .awaitTermination())

if __name__ == "__main__":
    main()
