import axios from "axios";
import { Kafka, logLevel } from "kafkajs";
import dotenv from "dotenv";
import dayjs from "dayjs";
import { Utils } from "./utils.js";
import { RedditService } from "./reddit.service.js";

// Initialize environment variables
dotenv.config();

/**
 * Service Configuration
 * Loads settings from environment variables with defaults.
 */
const CONFIG = {
  KAFKA_SERVERS: (
    process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092"
  ).split(","),
  USER_AGENT: process.env.REDDIT_USER_AGENT || "trend-insight/0.1",
  POLL_INTERVAL: parseInt(process.env.REDDIT_POLL_INTERVAL_SECONDS || "15"),
  POST_LIMIT: parseInt(process.env.REDDIT_LIMIT || "20000"),
  API_BASE_URL: process.env.API_BASE_URL || "http://trend-api:8000",
};

const kafka = new Kafka({
  clientId: "ingestion-service",
  brokers: CONFIG.KAFKA_SERVERS,
  logLevel: logLevel.ERROR,
});

console.log(
  `[ingestion] Config loaded: POLL_INTERVAL=${CONFIG.POLL_INTERVAL}s, POST_LIMIT=${CONFIG.POST_LIMIT}`,
);

/**
 * Periodic worker that syncs recent activity for all topics.
 * Fetches active topics and ingests posts from the last 48 hours.
 * Runs in an infinite loop with a sleep interval.
 */
async function syncTopicsPosts() {
  console.log("[ingestion] Starting Topic-Based Sync Worker...");
  const producer = await Utils.getProducer(kafka);

  while (true) {
    try {
      // Fetch the latest topic definitions from the API
      const res = await axios.get(`${CONFIG.API_BASE_URL}/topics`);
      const topics = (res.data as any[]) || [];
      const activeTopics = topics.filter((t: any) => t.is_active);

      for (const topic of activeTopics) {
        try {
          console.log(`[ingestion] Syncing topic: ${topic.title || topic.id}`);

          // Sync logic: Fetch posts from the last 48 hours
          // Using dayjs().unix() for consistent seconds timestamp
          const cutoff = dayjs().unix() - 48 * 60 * 60;

          await RedditService.ingestTopicPosts(producer, topic, CONFIG, {
            cutoffTimestamp: cutoff,
            maxLimit: CONFIG.POST_LIMIT,
          });
        } catch (e: any) {
          console.error(
            `[ingestion] Error syncing topic ${topic.id}:`,
            e.message,
          );
        }
      }
    } catch (e: any) {
      console.error(`[ingestion] Sync cycle error:`, e.message);
    }

    // Wait 1 minute between sync cycles
    await Utils.sleep(60 * 1000);
  }
}

/**
 * Worker to handle historical backfill requests.
 * Listens to 'reddit.tasks.backfill' Kafka topic.
 * Performs deep storage ingestion for requested topics (up to 30 days).
 */
async function startBackfilling() {
  // Delay for Spark to load TOPIC_CACHE
  console.log(
    "[backfill] Starting worker. Waiting 3s for system stabilization...",
  );
  await Utils.sleep(3000);

  const consumer = kafka.consumer({
    groupId: "ingestion-backfill-worker",
    sessionTimeout: 120000,
    heartbeatInterval: 30000,
    rebalanceTimeout: 120000,
  });
  const producer = await Utils.getProducer(kafka);

  await consumer.connect();
  await consumer.subscribe({ topic: "reddit.tasks.backfill" });

  await consumer.run({
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      isRunning,
      isStale,
    }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;

        try {
          const rawValue = message.value?.toString();
          if (!rawValue) continue;

          // Parse backfill payload
          const payload = JSON.parse(rawValue);
          const { topic_id } = payload;

          // Retrieve full topic details to get keywords and subreddits
          const res = await axios.get(
            `${CONFIG.API_BASE_URL}/topics/${topic_id}`,
          );
          const topic = res.data as any;

          // Calculate cutoff timestamp (Fixed 30 days ago for backfills)
          const cutoff = dayjs().unix() - 30 * 24 * 3600;

          // Invoke ingestion service with progress tracking
          await RedditService.ingestTopicPosts(producer, topic, CONFIG, {
            cutoffTimestamp: cutoff,
            maxLimit: CONFIG.POST_LIMIT,
            heartbeat: async () => await heartbeat(),
            onProgress: async (pct: number) => {
              // Updates topic status with percentage in DB
              await axios.patch(
                `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
                {
                  percentage: Math.round(pct),
                },
              );
            },
          });

          // Mark as COMPLETED upon success
          await axios.patch(
            `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
            {
              status: "COMPLETED",
              percentage: 100,
            },
          );

          resolveOffset(message.offset);
          await heartbeat();
        } catch (err: any) {
          console.error(`[backfill] Task error:`, err.message);
        }
      }
    },
  });
}

syncTopicsPosts();
startBackfilling();
