import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, struct, to_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType
from textblob import TextBlob

# Kafka & Config
KAFKA_URL = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Define Schema for Reddit Topic Matches (from Ingestion)
# Includes fields added by Ingestion: engagement, timestamp, event_type
POST_SCHEMA = StructType([
    StructField("topic_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("created_utc", StringType(), True),
    StructField("text", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("subreddit", StringType(), True),
    StructField("author", StringType(), True),
    StructField("engagement", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True)
])

# UDF for Sentiment Analysis
def analyze_sentiment(text):
    return TextBlob(text).sentiment.polarity if text else 0.0

sentiment_udf = udf(analyze_sentiment, FloatType())

if __name__ == "__main__":
    # Create Spark Driver
    spark = (SparkSession.builder
        .appName("SentimentAnalyzer")
        .getOrCreate())

    # Show only warnings and errors in the logs
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("subscribe", "topic.matched.posts")
        .option("kafka.group.id", "enrich-matched-posts")
        .option("startingOffsets", "earliest")
        .load())

    # 2. Parse JSON
    parsed_stream = (raw_stream.select(
        from_json(col("value").cast("string"), POST_SCHEMA).alias("data")
    ).select("data.*"))

    # 3. Apply Transformations
    enriched_stream = (parsed_stream
        .withColumn("sentiment_score", sentiment_udf(col("text"))))

    # 4. Write to Kafka
    # Logstash will consume from this topic.
    query = (enriched_stream
        .select(to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_URL)
        .option("topic", "topic.enriched.matched.posts")
        .option("checkpointLocation", "/checkpoints/sentiment_analysis")
        .outputMode("append")
        .start())

    query.awaitTermination()
