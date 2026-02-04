import json
import os
import re
import sqlite3
import shutil
from datetime import datetime
from typing import List, Dict, Any

from pyspark.sql import SparkSession, functions as F

# =============================================================================
# 1. SETTINGS
# =============================================================================
# Kafka details
KAFKA_URL      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL   = "/data/topics.db"
CHECKPOINT_DIR = "/checkpoints"

# Data Channels
RAW_POSTS_DATA   = "reddit.raw.posts"
RAW_COMMENTS_DATA = "reddit.raw.comments"
MATCHED_DATA_OUT = "reddit.topic.matched.v2"
FINAL_METRICS_OUT = "reddit.topic.metrics"

# =============================================================================
# 2. DATA SHAPE (The Schema)
# =============================================================================
# We define our data using simple SQL-style descriptions
POST_SCHEMA    = "event_id STRING, created_utc STRING, text STRING, score INT, num_comments INT"
MATCHED_SCHEMA = f"{POST_SCHEMA}, topic_id STRING, term STRING"

# =============================================================================
# 3. HELPER: Get Topics from Database
# =============================================================================

def get_topics_to_watch():
    """Reads your topics and keywords from the database."""
    if not os.path.exists(DATABASE_URL): return []
    
    # Copy DB locally to prevent "Database Locked" errors
    try:
        shutil.copyfile(DATABASE_URL, "/tmp/topics.db")
    except:
        pass

    topics = []
    with sqlite3.connect("/tmp/topics.db") as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT id, keywords FROM topics WHERE is_active = 1").fetchall()
        for r in rows:
            raw = (r["keywords"] or "").strip()
            # Handle both JSON: ["war", "news"] and String: "war, news" formats
            try:
                kws = json.loads(raw) if raw.startswith("[") else [x.strip().lower() for x in raw.split(",") if x]
                topics.append({"id": r["id"], "keywords": kws})
            except:
                continue
    return topics

# =============================================================================
# 4. JOB 1: THE MATCHER
# =============================================================================

def find_topic_matches(batch_df, batch_id):
    """Checks every Reddit post against your list of topics."""
    topics = get_topics_to_watch()
    if not topics: return

    # A. Search Function
    @F.udf(returnType="array<struct<topic_id:string, term:string>>")
    def scanner(text):
        if not text: return []
        found_matches = []
        for t in topics:
            # Create a simple "OR" pattern: (word1|word2|...)
            pattern = "|".join([re.escape(str(k)) for k in t["keywords"]])
            regex = re.compile(f"(?i)({pattern})")
            
            # Find all matching words in the text
            if regex.search(text):
                found_terms = regex.findall(text)
                for word in set([w.lower() for w in found_terms]):
                    found_matches.append((t["id"], word))
        return found_matches

    # B. Apply Scanner & Expand
    # If a post matches 3 topics, we create 3 rows so each topic gets a count!
    matched_results = (
        batch_df
        .withColumn("matches", scanner(F.col("text")))
        .filter("size(matches) > 0")  # Only keep matches
        .selectExpr("*", "explode(matches) as match")
        .selectExpr("*", "match.topic_id", "match.term")
        .drop("matches", "match")
    )

    # C. Send to Matched Channel
    (matched_results.select(F.to_json(F.struct("*")).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", KAFKA_URL)
     .option("topic", MATCHED_DATA_OUT)
     .save())

# =============================================================================
# 5. MAIN PIPELINE
# =============================================================================

def start_pipeline():
    spark = SparkSession.builder.appName("RedditMatchCounter").getOrCreate()
    
    # --- PHASE 1: READ & MATCH ---
    # Listen to raw Reddit posts and call 'find_topic_matches'
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", f"{RAW_POSTS_DATA},{RAW_COMMENTS_DATA}")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), POST_SCHEMA).alias("data"))
        .select("data.*")
    )

    matcher = (
        raw_stream.writeStream
        .foreachBatch(find_topic_matches)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/mather_v1")
        .start()
    )

    # --- PHASE 2: AGGREGATE ---
    # Read matches -> Group by Day -> Send to API
    counter_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", MATCHED_DATA_OUT)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), MATCHED_SCHEMA).alias("data"))
        .select("data.*")
        
        # 1. Deduplicate: Don't count the same post twice!
        .dropDuplicates(["event_id", "topic_id"])
        
        # 2. Format Time
        .withColumn("time", F.col("created_utc").cast("timestamp"))
        .withWatermark("time", "35 days")
        
        # 3. Group by Day + Topic
        .groupBy("topic_id", F.window("time", "1 day").alias("day"))
        .agg(
            F.count("*").alias("mentions"),
            F.sum(F.col("score") + F.coalesce(F.col("num_comments"), F.lit(0))).alias("engagement")
        )
        
        # 4. Prepare Final Format
        .selectExpr(
            "topic_id", "mentions", "engagement",
            "day.start as start", "day.end as end",
            "'1d' as window_type"
        )
    )

    metrics_pusher = (
        counter_stream.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("topic", FINAL_METRICS_OUT)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/metrics_v1")
        .outputMode("update")
        .start()
    )

    print("Pipeline started successfully.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    start_pipeline()
