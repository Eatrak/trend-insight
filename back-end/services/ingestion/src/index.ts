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

// Kafka Client Setup
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
  const consumer = kafka.consumer({ groupId: "ingestion-backfill-worker" });
  const producer = await Utils.getProducer(kafka);

  // Establish connection and subscribe to the backfill task queue
  await consumer.connect();
  await consumer.subscribe({ topic: "reddit.tasks.backfill" });

  // Start the consumer loop to process backfill requests
  await consumer.run({
    eachMessage: async ({ message }) => {
      // Parse the task payload containing topic ID and subreddits to scrape
      const payload = JSON.parse(message.value?.toString() || "{}");
      const { topic_id, subreddits = [] } = payload;

      if (!subreddits.length) return;

      // Configuration for the backfill window and limits
      const MAX_FETCH_LIMIT = 5000; // Safety cap on total posts per topic
      const LOOKBACK_SECONDS = 7 * 24 * 3600; // We look back 7 days
      const cutoff = dayjs().unix() - LOOKBACK_SECONDS;
      const subredditsQuery = subreddits.slice(0, 100).join("+");

      let after: string | null = null;
      let fetchedCount = 0;
      let isFinished = false;

      // Iterate through Reddit's paginated results
      while (!isFinished && fetchedCount < MAX_FETCH_LIMIT) {
        try {
          // Request the latest posts from the combined subreddits
          const response = await RedditService.request(
            `https://www.reddit.com/r/${subredditsQuery}/new.json`,
            { limit: 100, after },
            CONFIG.USER_AGENT,
          );

          const posts = response.data?.children || [];
          if (!posts.length) break;

          for (const post of posts) {
            const postToStore = Utils.extractContent(post, "post");

            // Determine if we've reached posts older than our cutoff time
            const isPastCutoff =
              !postToStore || dayjs(postToStore.created_utc).unix() < cutoff;

            if (isPastCutoff) {
              isFinished = true;
              break;
            }

            // Forward the raw post data to the ingestion pipeline via Kafka
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

          // Track the 'after' token for the next page of results
          after = lastPost.data.name;

          // Calculate progress percentage based on the age of the last fetched post, relative to the backfill window
          const lastPostTimestamp = posts[posts.length - 1].data.created_utc;
          const progress = Math.min(
            100,
            Math.max(
              0,
              ((dayjs().unix() - lastPostTimestamp) / LOOKBACK_SECONDS) * 100,
            ),
          );

          // Report current progress back to the API for UI updates
          await axios.patch(
            `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
            {
              percentage: progress,
            },
          );

          // Respect Reddit API rate limits with a short delay between batches
          await Utils.sleep(1500);
        } catch (error) {
          console.error(
            `[backfill] Error processing batch for topic ${topic_id}:`,
            error,
          );
          break;
        }
      }

      // Finalize the task status once processing is done or limit is reached
      await axios.patch(`${CONFIG.API_BASE_URL}/topics/${topic_id}/status`, {
        status: "COMPLETED",
        percentage: 100,
      });
    },
  });
}

startRealTimeIngestion();
startBackfilling();
