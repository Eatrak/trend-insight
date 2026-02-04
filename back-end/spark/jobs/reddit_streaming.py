import json
import os
import re
import sqlite3
import shutil
from pyspark.sql import SparkSession, functions as F

# =============================================================================
# 1. SETTINGS
# =============================================================================
KAFKA_URL         = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL      = "/data/topics.db"
CHECKPOINT_DIR    = "/checkpoints"

# Output Channels
INTERMEDIATE_TOPIC = "reddit.topic.matched.v2"
DAILY_METRICS_OUT  = "reddit.topic.metrics"

# Data Definitions
POST_SCHEMA = "event_id STRING, created_utc STRING, text STRING, score INT, num_comments INT"

# =============================================================================
# 2. HELPER: Get Keywords from DB
# =============================================================================

def get_topics_to_watch():
    """Reads active topics and keywords from the database."""
    if not os.path.exists(DATABASE_URL): return []
    try:
        shutil.copyfile(DATABASE_URL, "/tmp/topics.db")
    except: pass

    topics = []
    with sqlite3.connect("/tmp/topics.db") as conn:
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("SELECT id, keywords FROM topics WHERE is_active = 1").fetchall()
            for r in rows:
                raw = (r["keywords"] or "").strip()
                kws = json.loads(raw) if raw.startswith("[") else [x.strip().lower() for x in raw.split(",") if x]
                topics.append({"id": r["id"], "keywords": kws})
        except Exception:
            pass
    return topics

# =============================================================================
# 3. MAIN PIPELINE
# =============================================================================

def main():
    spark = SparkSession.builder.appName("UnifiedStreamer").getOrCreate()
    
    # --- STEP 1: Process Raw Data & Identify Initial Matches ---
    # We use foreachBatch here to efficiently load topic configs once per batch.
    
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", "reddit.raw.posts,reddit.raw.comments")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("data"))
        .select("data.*")
    )

    def match_logic(batch_df, batch_id):
        topics = get_topics_to_watch()
        if not topics: return

        # Pre-compile regex patterns for efficiency
        topic_configs = []
        for t in topics:
            regex = re.compile(f"(?i)({'|'.join([re.escape(str(k)) for k in t['keywords']])})")
            topic_configs.append((t["id"], regex))
            
        @F.udf(returnType="array<struct<topic_id:string, term:string>>")
        def scan_text(text):
            if not text: return []
            matches = []
            for tid, regex in topic_configs:
                if regex.search(text):
                    # Find all unique matches
                    found_terms = set(regex.findall(text))
                    matches.extend([(tid, w) for w in found_terms])
            return list(set(matches)) # Ensure uniqueness

        # Apply scan and explode matches
        tagged = (
            batch_df.withColumn("matches", scan_text(F.col("text")))
            .filter("size(matches) > 0")
            .selectExpr("*", "explode(matches) as m")
            .selectExpr("*", "m.topic_id", "m.term")
            .drop("matches", "m")
        )
        
        # Write matches to intermediate internal topic
        (tagged.select(F.to_json(F.struct("*")).alias("value"))
         .write.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_URL)
         .option("topic", INTERMEDIATE_TOPIC)
         .save())

    # Start the Matcher Stream
    spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_URL) \
        .option("subscribe", "reddit.raw.posts,reddit.raw.comments") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("data")) \
        .select("data.*") \
        .writeStream \
        .foreachBatch(match_logic) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/matcher_v2") \
        .start()

    # --- STEP 2: Aggregate Metrics (Daily Totals) ---
    # Read from intermediate topic -> Deduplicate -> Aggregate
    
    matched_schema = f"{POST_SCHEMA}, topic_id STRING, term STRING"
    
    (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", INTERMEDIATE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), matched_schema).alias("data"))
        .select("data.*")
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
        .select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("topic", DAILY_METRICS_OUT)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/metrics_v3")
        .outputMode("update")
        .start())

    print("Spark Streaming Pipeline Started.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
