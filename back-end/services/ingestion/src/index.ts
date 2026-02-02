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
 */
const CONFIG = {
  KAFKA_SERVERS: (
    process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092"
  ).split(","),
  USER_AGENT: process.env.REDDIT_USER_AGENT || "trend-insight/0.1",
  POLL_INTERVAL: parseInt(process.env.REDDIT_POLL_INTERVAL_SECONDS || "15"),
  POST_LIMIT: parseInt(process.env.REDDIT_LIMIT || "50"),
  API_BASE_URL: process.env.API_BASE_URL || "http://trend-api:8000",
};

const kafka = new Kafka({
  clientId: "ingestion-service",
  brokers: CONFIG.KAFKA_SERVERS,
  logLevel: logLevel.ERROR,
});

/**
 * Worker for real-time data ingestion.
 */
async function startRealTimeIngestion() {
  const producer = await Utils.getProducer(kafka);
  const lastSeen = { post: new Set<string>(), comment: new Set<string>() };

  while (true) {
    try {
      const res = await axios.get(`${CONFIG.API_BASE_URL}/subreddits`, {
        timeout: 5000,
      });
      const subreddits = (res.data as any).subreddits || [];

      if (subreddits.length > 0) {
        await RedditService.poll(
          producer,
          subreddits.slice(0, 100),
          lastSeen,
          CONFIG,
        );
        await Utils.sleep(2000);
      }

      for (const key in lastSeen) {
        const set = (lastSeen as any)[key];
        if (set.size > 20000)
          (lastSeen as any)[key] = new Set(Array.from(set).slice(-10000));
      }
    } catch (e: any) {
      console.error(`[ingestion] Poll error:`, e.message);
    }
    await Utils.sleep(CONFIG.POLL_INTERVAL * 1000);
  }
}

/**
 * Worker to handle historical backfill requests.
 */
async function startBackfilling() {
  const consumer = kafka.consumer({
    groupId: "ingestion-backfill-worker",
    // 2 mins. How long Kafka is willing to wait before it gives up on the worker.
    // By default it's 30 seconds, but if a request to Reddit takes longer than that, Kafka will consider the worker as failed.
    sessionTimeout: 120000,
    // 30s. How often the worker intends to check in.
    // We also use manual heartbeats during active backfilling,
    // because during backfilling the worker is busy and doesn't want to check in.
    heartbeatInterval: 30000,
    // 2 mins. To allow long-running backfills to complete gracefully before Kafka reorders workers
    rebalanceTimeout: 120000,
  });
  const producer = await Utils.getProducer(kafka);

  // Establish connection and subscribe to the backfill task queue
  await consumer.connect();
  await consumer.subscribe({ topic: "reddit.tasks.backfill" });

  // Start the consumer loop to process backfill requests
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

          const payload = JSON.parse(rawValue);
          const { topic_id, subreddits = [] } = payload;

          console.log(
            `[backfill] Topic: ${topic_id} | Subreddits: ${subreddits.join(", ")}`,
          );

          if (!Array.isArray(subreddits) || !subreddits.length) {
            await axios.patch(
              `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
              { status: "COMPLETED", percentage: 100 },
            );
            resolveOffset(message.offset);
            continue;
          }

          const MAX_FETCH_LIMIT = 5000;
          const LOOKBACK_SECONDS = 7 * 24 * 3600;
          const cutoff = dayjs().unix() - LOOKBACK_SECONDS;
          const subredditsQuery = subreddits.slice(0, 100).join("+");

          let after: string | null = null;
          let fetchedCount = 0;
          let isFinished = false;

          while (!isFinished && fetchedCount < MAX_FETCH_LIMIT) {
            try {
              // Send heartbeat to Kafka to keep the session alive
              await heartbeat();

              const response = await RedditService.request(
                `https://www.reddit.com/r/${subredditsQuery}/new.json`,
                { limit: 100, after },
                CONFIG.USER_AGENT,
              );

              if (!response || !response.data) break;

              const posts = response.data.children || [];
              if (!posts.length) break;

              for (const post of posts) {
                const postToStore = Utils.extractContent(post, "post");
                const isPastCutoff =
                  !postToStore ||
                  dayjs(postToStore.created_utc).unix() < cutoff;

                if (isPastCutoff) {
                  isFinished = true;
                  break;
                }

                await producer.send({
                  topic: "reddit.raw.posts",
                  messages: [
                    {
                      key: postToStore.event_id,
                      value: JSON.stringify(postToStore),
                    },
                  ],
                });
              }

              fetchedCount += posts.length;
              let lastPost = posts[posts.length - 1];
              after = lastPost.data.name;

              const progress = Math.min(
                100,
                Math.max(
                  0,
                  ((dayjs().unix() - lastPost.data.created_utc) /
                    LOOKBACK_SECONDS) *
                    100,
                ),
              );
              await axios.patch(
                `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
                { percentage: progress },
              );

              await Utils.sleep(1500);
            } catch (error: any) {
              console.error(
                `[backfill] Error in batch for ${topic_id}:`,
                error.message,
              );
              break;
            }
          }

          await axios.patch(
            `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
            { status: "COMPLETED", percentage: 100 },
          );
          resolveOffset(message.offset);
          await heartbeat();
        } catch (err: any) {
          console.error(
            `[backfill] Critical error processing message:`,
            err.message,
          );
        }
      }
    },
  });
}

startRealTimeIngestion();
startBackfilling();
