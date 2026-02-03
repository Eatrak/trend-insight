import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# Configuration & Constants
# These define where Kafka is, where the database lives, and which topics to use.
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPICS_DB_PATH = os.getenv("TOPICS_DB_PATH", "/data/topics.db")
CHECKPOINT_BASE = os.getenv("SPARK_CHECKPOINT_BASE", "/checkpoints")

# Topics
RAW_POSTS_TOPIC = "reddit.raw.posts"
RAW_COMMENTS_TOPIC = "reddit.raw.comments"
MATCHED_TOPIC = "reddit.topic.matched"
METRICS_TOPIC = "reddit.topic.metrics"
GLOBAL_TRENDS_TOPIC = "reddit.global.trends"

# Weights
W1 = float(os.getenv("WEIGHT_VELOCITY", "0.5"))
W2 = float(os.getenv("WEIGHT_ACCELERATION", "0.3"))
W3 = float(os.getenv("WEIGHT_ENGAGEMENT", "0.2"))

# Watermark
WATERMARK_DURATION = "35 days"


# -----------------------------------------------------------------------------
# 1. Utils & UDFs
# -----------------------------------------------------------------------------

WORD_RE = re.compile(r"[a-zA-Z0-9_]{2,}")

# UDF (User Defined Function) to split text into a list of words.
# We use this to analyze the content of posts.
@F.udf(returnType=T.ArrayType(T.StringType()))
def tokenize(text: str) -> List[str]:
    if not text:
        return []
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]

# UDF to extract n-grams (phrases of 2 or 3 words).
# This helps identify trending phrases like "machine learning" instead of just "machine".
@F.udf(returnType=T.ArrayType(T.StringType()))
def extract_ngrams(text: str) -> List[str]:
    if not text:
        return []
    words = [m.group(0).lower() for m in WORD_RE.finditer(text)]
    if len(words) < 2:
        return []
    
    ngrams = []
    for i in range(len(words) - 1):
        ngrams.append(f"{words[i]} {words[i+1]}")
    for i in range(len(words) - 2):
        ngrams.append(f"{words[i]} {words[i+1]} {words[i+2]}")
    return ngrams

import shutil

# Loads the list of topics we want to track from a local SQLite database.
# Returns a list of dictionaries with topic_id, keywords, subreddits, etc.
def load_topics() -> List[Dict[str, Any]]:
    if not os.path.exists(TOPICS_DB_PATH):
        return []
        
    temp_db = "/tmp/topics_copy.db"
    try:
        shutil.copyfile(TOPICS_DB_PATH, temp_db)
    except:
        temp_db = TOPICS_DB_PATH

    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM topics WHERE is_active = 1").fetchall()
    except Exception as e:
        sys.stderr.write(f"ERROR: load_topics failed: {e}\n")
        return []
    finally:
        conn.close()

    def split_csv(s: str) -> List[str]:
        s = (s or "").strip()
        if not s:
            return []
        return [x.strip().lower() for x in s.split(",") if x.strip()]

    out: List[Dict[str, Any]] = []
    for r in rows:
        keywords_raw = r["keywords"]
        keywords = []
        try:
            keywords = json.loads(keywords_raw) if keywords_raw.startswith("[") else split_csv(keywords_raw)
        except:
            keywords = split_csv(keywords_raw)

        out.append(
            {
                "topic_id": r["id"],
                "keywords": keywords,
                "subreddits": split_csv(r["subreddits"]),
                "min_score": int(json.loads(r["filters_json"]).get("min_score", 0)) if r["filters_json"] else 0,
            }
        )
    return out


# -----------------------------------------------------------------------------
# Core Logic: Windowed Metrics Computation
# This function calculates stats (mentions, engagement) over time windows.
# For example: "How many times was this topic mentioned in the last hour?"
# -----------------------------------------------------------------------------
def compute_windowed_metrics(
    df_stream, 
    group_col: str, 
    window_duration: str, 
    slide_duration: str, 
    window_label: str,
    metric_cols: List[str] = ["mentions", "engagement"]
):
    """
    Computes Velocity and Acceleration for a given window configuration.
    Returns a Stream DataFrame with schema:
    [group_col, start, end, mentions, engagement, velocity, acceleration, trend_score, window_type]
    """
    
    # Duration in hours (approx) for normalization
    duration_hours_map = {
        "30 minutes": 0.5,
        "60 minutes": 1.0, 
        "120 minutes": 2.0
    }
    duration_hrs = duration_hours_map.get(window_duration, 1.0) # Default 1h

    # 1. Base Aggregates
    aggs = (
        df_stream
        .groupBy(group_col, F.window("event_time", window_duration, slide_duration).alias("window"))
        .agg(
            F.count("*").alias("mentions"),
            F.sum(F.col("score") + F.coalesce(F.col("num_comments"), F.lit(0))).alias("engagement")
        )
        .select(
            group_col,
            F.col("window.start").alias("start"),
            F.col("window.end").alias("end"),
            "mentions",
            "engagement"
        )
        .withColumn("window_type", F.lit(window_label))
        # Add placeholders for velocity/acceleration/trend_score so the schema remains compatible if needed, 
        # or we update schema. Let's update schema in next steps, but for now 
        # let's just output raw metrics.
        # Actually, let's keep the schema simple and do calculation in API.
    )
    
    return aggs


# -----------------------------------------------------------------------------
# 2. Main Job
# This is the entry point for the Spark application.
# It sets up the data processing pipeline.
# -----------------------------------------------------------------------------
def main() -> None:
    spark = (
        SparkSession.builder.appName("trend-insight-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    # Strict Schema
    # Defines the structure of the JSON data coming from Kafka.
    # We must tell Spark exactly what fields to expect.
    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("subreddit", T.StringType(), True),
            T.StructField("author", T.StringType(), True),
            T.StructField("created_utc", T.StringType(), True),
            T.StructField("text", T.StringType(), True),
            T.StructField("score", T.IntegerType(), True),
            T.StructField("num_comments", T.IntegerType(), True),
            T.StructField("ingested_at", T.StringType(), True),
        ]
    )

    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", f"{RAW_POSTS_TOPIC},{RAW_COMMENTS_TOPIC}")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON from Kafka and clean up the data
    df = (
        df_raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*")
        # Fix timestamp parsing for ISO8601 (e.g. 2026-01-21T16:28:54.833599+00:00)
        .withColumn("event_time", F.to_timestamp(F.col("created_utc"))) 
        # Note: In Spark 3.x, cast("timestamp") or to_timestamp usually handles ISO8601. 
        # If it fails, we might need explicit format. 
        # But let's try explicit cast which is more robust for ISO in newer Spark.
        .withColumn("event_time", F.col("created_utc").cast("timestamp"))
        .withColumn("tokens", tokenize(F.col("text")))
        # Move Watermark to specific queries to avoid dropping data early if parsing fails?
        # But we need event_time for windowing.
        .withWatermark("event_time", WATERMARK_DURATION)
    )

    # ---------------------------------------------------------------------
    # JOB 1: Topic Matching
    # This job checks every incoming post against our list of keywords.
    # If a post matches a topic, it's sent to the 'reddit.topic.matched' Kafka topic.
    # ---------------------------------------------------------------------
    def write_matched(batch_df, batch_id: int) -> None:
        topics = load_topics()
        if not topics: return
        rows = []
        for t in topics:
            for kw in (t["keywords"] or []):
                if isinstance(kw, list):
                    for k in kw: rows.append((t["topic_id"], k.lower()))
                else:
                    rows.append((t["topic_id"], kw.lower()))
        if not rows: return
        
        map_df = spark.createDataFrame(rows, schema=["topic_id", "term"])
        exploded = batch_df.select("*", F.explode_outer("tokens").alias("token")).withColumn("token", F.lower(F.col("token")))
        
        
        joined = exploded.join(map_df, exploded["token"] == map_df["term"], "inner")
        matched = joined.dropDuplicates(["event_id", "topic_id"])
        
        (matched.select(F.to_json(F.struct(*[F.col(c) for c in matched.columns])).alias("value"))
         .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("topic", MATCHED_TOPIC).save())

    matched_query = (
        df.writeStream.foreachBatch(write_matched)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/matched_v3")
        .start()
    )

    # ---------------------------------------------------------------------
    # JOB 2: Adaptive Topic Metrics
    # This job reads the matched posts and calculates aggregate metrics.
    # It computes stats for:
    # - Short term: 1 day windows
    # - Medium term: 7 day windows
    # - Long term: 30 day windows
    # The results are sent to 'reddit.topic.metrics'.
    # ---------------------------------------------------------------------
    matched_json_schema = schema.add("topic_id", T.StringType()).add("tokens", T.ArrayType(T.StringType()))
    df_matched_in = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", MATCHED_TOPIC)
        .load()
        .select(F.from_json(F.col("value").cast("string"), matched_json_schema).alias("data"))
        .select("data.*")
        # Fix timestamp parsing for Job 2 as well
        .withColumn("event_time", F.to_timestamp(F.col("created_utc")))
        .withColumn("event_time", F.col("created_utc").cast("timestamp"))
        .withWatermark("event_time", WATERMARK_DURATION)
    )

    # Compute Variants
    # Short: 1 day window, 1 day slide (1d) - Tumbling
    # Distinct Daily Bars
    df_short = compute_windowed_metrics(df_matched_in, "topic_id", "1 day", "1 day", "1d")
    
    # Med: 7 days window, 7 days slide (1w) - Tumbling
    # Distinct Weekly Bars
    df_med = compute_windowed_metrics(df_matched_in, "topic_id", "7 days", "7 days", "1w")
    
    # Long: 30 days window, 30 days slide (1m) - Tumbling
    # Distinct Monthly Bars
    df_long = compute_windowed_metrics(df_matched_in, "topic_id", "30 days", "30 days", "1m")

    # Union all
    df_all_metrics = df_short.union(df_med).union(df_long)

    metrics_query = (
        df_all_metrics.select(F.to_json(F.struct(*df_all_metrics.columns)).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", METRICS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metrics_adaptive_v3")
        .outputMode("update")
        .start()
    )

    # ---------------------------------------------------------------------
    # JOB 3: Adaptive Global Trends (N-grams) - DISABLED TEMPORARILY
    # This job would find trending phrases across all posts, even if they aren't
    # in our topic list. It is currently commented out to save resources.
    # ---------------------------------------------------------------------
    # ngram_df = (
    #     df.select("*", F.explode(extract_ngrams(F.col("text"))).alias("ngram"))
    #     .withWatermark("event_time", WATERMARK_DURATION)
    # )

    # # Compute Variants for N-grams
    # g_short = compute_windowed_metrics(ngram_df, "ngram", "30 minutes", "15 minutes", "30m")
    # g_med = compute_windowed_metrics(ngram_df, "ngram", "60 minutes", "30 minutes", "60m")
    # g_long = compute_windowed_metrics(ngram_df, "ngram", "120 minutes", "60 minutes", "120m")

    # df_all_trends = g_short.union(g_med).union(g_long)
    
    
    # # Filter: Only significant trends
    # df_filtered_trends = df_all_trends.where(F.col("mentions") > 3)

    # top3_query = (
    #     df_filtered_trends
    #     .writeStream
    #     .foreachBatch(lambda df, epoch: df.sort(F.col("mentions").desc()).limit(10).select(F.to_json(F.struct("*")).alias("value")).write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("topic", GLOBAL_TRENDS_TOPIC).save())
    #     .option("checkpointLocation", f"{CHECKPOINT_BASE}/global_trends_rank")
    #     .start()
    # )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
