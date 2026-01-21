import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# Configuration & Constants
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
WATERMARK_DURATION = "10 minutes"


# -----------------------------------------------------------------------------
# 1. Utils & UDFs
# -----------------------------------------------------------------------------

WORD_RE = re.compile(r"[a-zA-Z0-9_]{2,}")

@F.udf(returnType=T.ArrayType(T.StringType()))
def tokenize(text: str) -> List[str]:
    if not text:
        return []
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]

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

def load_topics() -> List[Dict[str, Any]]:
    if not os.path.exists(TOPICS_DB_PATH):
        return []
    conn = sqlite3.connect(TOPICS_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM topics WHERE is_active = 1").fetchall()
    except Exception:
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
    )

    # 2. Velocity (Current vs Previous)
    # Join on: Prev.start = Curr.start - Slide
    curr = aggs.withColumnRenamed("mentions", "m_curr").withColumnRenamed("engagement", "e_curr")
    prev = aggs.withColumnRenamed("mentions", "m_prev").withColumnRenamed("engagement", "e_prev").withColumnRenamed(group_col, "grp_prev").withColumnRenamed("start", "s_prev")

    # Dynamic Interval string for Join
    interval_expr = f"INTERVAL {slide_duration}"

    velocity_df = (
        curr.join(
            prev,
            (curr[group_col] == prev.grp_prev) & 
            (prev.s_prev == (curr.start - F.expr(interval_expr))),
            "left_outer"
        )
        .select(
            curr[group_col],
            curr.start,
            curr.end,
            F.coalesce(curr.m_curr, F.lit(0)).alias("mentions"),
            F.coalesce(curr.e_curr, F.lit(0)).alias("engagement"),
            # Velocity = (Curr - Prev) / Duration
            (
                (F.coalesce(curr.m_curr, F.lit(0)) - F.coalesce(prev.m_prev, F.lit(0))) 
                / duration_hrs
            ).alias("velocity")
        )
    )

    # 3. Acceleration (Curr Velocity vs Prev Velocity)
    v_curr = velocity_df.withColumnRenamed("velocity", "v_curr")
    v_prev = velocity_df.withColumnRenamed("velocity", "v_prev").withColumnRenamed("mentions", "m_prev").withColumnRenamed("engagement", "e_prev").withColumnRenamed(group_col, "g_prev").withColumnRenamed("start", "st_prev")

    accel_df = (
        v_curr.join(
            v_prev,
            (v_curr[group_col] == v_prev.g_prev) &
            (v_prev.st_prev == (v_curr.start - F.expr(interval_expr))),
            "left_outer"
        )
        .select(
            v_curr[group_col],
            v_curr.end.alias("timestamp"),
            v_curr.mentions,
            v_curr.engagement,
            v_curr.v_curr.alias("velocity"),
            # Acceleration = V_curr - V_prev
            (v_curr.v_curr - F.coalesce(v_prev.v_prev, F.lit(0.0))).alias("acceleration")
        )
        .withColumn(
            "trend_score",
            (F.lit(W1) * F.col("velocity")) + 
            (F.lit(W2) * F.col("acceleration")) + 
            (F.lit(W3) * F.col("engagement"))
        )
        .withColumn("window_type", F.lit(window_label))
    )
    
    return accel_df


# -----------------------------------------------------------------------------
# 2. Main Job
# -----------------------------------------------------------------------------
def main() -> None:
    spark = (
        SparkSession.builder.appName("reddit-insight-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    # Strict Schema
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
        .option("startingOffsets", "latest")
        .load()
    )

    df = (
        df_raw.select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*")
        .withColumn("event_time", F.to_timestamp("created_utc"))
        .withColumn("tokens", tokenize(F.col("text")))
        .withWatermark("event_time", WATERMARK_DURATION)
    )

    # ---------------------------------------------------------------------
    # JOB 1: Topic Matching (Pass-through, no change needed)
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
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/matched")
        .start()
    )

    # ---------------------------------------------------------------------
    # JOB 2: Adaptive Topic Metrics
    # (30m/15m, 60m/30m, 120m/60m)
    # ---------------------------------------------------------------------
    matched_json_schema = schema.add("topic_id", T.StringType()).add("tokens", T.ArrayType(T.StringType()))
    df_matched_in = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", MATCHED_TOPIC)
        .load()
        .select(F.from_json(F.col("value").cast("string"), matched_json_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("created_utc"))
        .withWatermark("event_time", WATERMARK_DURATION)
    )

    # Compute Variants
    # Short: 30m window, 15m slide
    df_short = compute_windowed_metrics(df_matched_in, "topic_id", "30 minutes", "15 minutes", "30m")
    # Med: 60m window, 30m slide
    df_med = compute_windowed_metrics(df_matched_in, "topic_id", "60 minutes", "30 minutes", "60m")
    # Long: 120m window, 60m slide
    df_long = compute_windowed_metrics(df_matched_in, "topic_id", "120 minutes", "60 minutes", "120m")

    # Union all
    df_all_metrics = df_short.union(df_med).union(df_long)

    metrics_query = (
        df_all_metrics.select(F.to_json(F.struct(*df_all_metrics.columns)).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", METRICS_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/metrics_adaptive")
        .outputMode("append")
        .start()
    )

    # ---------------------------------------------------------------------
    # JOB 3: Adaptive Global Trends (N-grams)
    # ---------------------------------------------------------------------
    ngram_df = (
        df.select("*", F.explode(extract_ngrams(F.col("text"))).alias("ngram"))
        .withWatermark("event_time", WATERMARK_DURATION)
    )

    # Compute Variants for N-grams
    g_short = compute_windowed_metrics(ngram_df, "ngram", "30 minutes", "15 minutes", "30m")
    g_med = compute_windowed_metrics(ngram_df, "ngram", "60 minutes", "30 minutes", "60m")
    g_long = compute_windowed_metrics(ngram_df, "ngram", "120 minutes", "60 minutes", "120m")

    df_all_trends = g_short.union(g_med).union(g_long)
    
    # Filter: Only significant trends
    df_filtered_trends = df_all_trends.where(F.col("mentions") > 3)

    top3_query = (
        df_filtered_trends
        .writeStream
        .foreachBatch(lambda df, epoch: df.sort(F.col("trend_score").desc()).limit(10).write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("topic", GLOBAL_TRENDS_TOPIC).save())
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/global_trends_rank")
        .start()
    )

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
