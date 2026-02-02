import axios from "axios";
import { Kafka, Producer, logLevel } from "kafkajs";
import dotenv from "dotenv";
import dayjs from "dayjs";
import { Utils } from "./utils.js";

// Initialize environment variables
dotenv.config();

/**
 * Service Configuration
 * Centralized settings for Kafka connection, Reddit polling, and internal API URLs.
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
 * Helper to make requests to Reddit with automatic 429 (Rate Limit) handling.
 */
async function redditRequest(url: string, params: any): Promise<any> {
  try {
    const response = await axios.get(url, {
      headers: { "User-Agent": CONFIG.USER_AGENT },
      params,
      timeout: 20000,
    });
    return response.data;
  } catch (error: any) {
    // If throttled, Reddit provides a wait time in headers. We wait and retry.
    if (error.response?.status === 429) {
      const retryAfter = parseInt(error.response.headers["retry-after"] || "5");
      console.warn(`[ingestion] Rate limited (429). Waiting ${retryAfter}s...`);
      await Utils.sleep(retryAfter * 1000);
      return redditRequest(url, params);
    }
    throw error;
  }
}

/**
 * Connects and returns a Kafka Producer. Retries indefinitely if the broker is unavailable.
 */
async function getProducer(): Promise<Producer> {
  const producer = kafka.producer();
  while (true) {
    try {
      await producer.connect();
      return producer;
    } catch (e: any) {
      console.error(
        `[ingestion] Kafka connection failed: ${e.message}. Retrying...`,
      );
      await Utils.sleep(5000);
    }
  }
}

/**
 * Polls Reddit for new posts and comments across a list of subreddits.
 */
async function pollReddit(producer: Producer, subs: string[], lastSeen: any) {
  const subStr = subs.join("+");
  for (const type of ["post", "comment"] as const) {
    try {
      const url = `https://www.reddit.com/r/${subStr}/${type === "post" ? "new" : "comments"}.json`;
      const data = await redditRequest(url, { limit: CONFIG.POST_LIMIT });
      const children = data.data?.children || [];

      for (const child of children) {
        const msg = Utils.extractContent(child, type);
        // Deduplication: Only send if we haven't seen this ID in the current session
        if (msg && !lastSeen[type].has(msg.event_id)) {
          await producer.send({
            topic: `reddit.raw.${type}s`,
            messages: [{ key: msg.event_id, value: JSON.stringify(msg) }],
          });
          lastSeen[type].add(msg.event_id);
        }
      }
      await Utils.sleep(2000); // Wait between batch types
    } catch (e: any) {
      console.error(`[ingestion] Error polling ${type}s:`, e.message);
    }
  }
}

/**
 * Main loop for real-time data ingestion.
 * Periodically fetches target subreddits and scans them for new content.
 */
async function startPolling() {
  const producer = await getProducer();
  // Use Sets to track content we've already ingested to avoid duplicates in the same session
  const lastSeen = { post: new Set<string>(), comment: new Set<string>() };

  while (true) {
    try {
      // Get subreddits currently being tracked by the system
      const res = await axios.get(`${CONFIG.API_BASE_URL}/subreddits`, {
        timeout: 5000,
      });
      const subreddits = (res.data as any).subreddits || [];

      if (subreddits.length === 0) {
        await Utils.sleep(CONFIG.POLL_INTERVAL * 1000);
        continue;
      }

      // Sync subreddits in chunks to avoid URL length limits
      for (let i = 0; i < subreddits.length; i += 50) {
        await pollReddit(producer, subreddits.slice(i, i + 50), lastSeen);
        await Utils.sleep(2000);
      }

      // Memory Management: Keep 'seen' sets from growing indefinitely
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
 * Triggers when a new topic is added to fetch the last 7 days of posts.
 */
async function startBackfill() {
  const consumer = kafka.consumer({ groupId: "ingestion-backfill-worker" });
  const producer = await getProducer();
  await consumer.connect();
  await consumer.subscribe({ topic: "reddit.tasks.backfill" });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const { topic_id, subreddits = [] } = JSON.parse(
        message.value?.toString() || "{}",
      );
      if (subreddits.length === 0) return;

      const cutoff = dayjs().unix() - 7 * 24 * 3600; // 7-day window
      for (let i = 0; i < subreddits.length; i += 50) {
        const chunk = subreddits.slice(i, i + 50).join("+");
        let after = null,
          fetched = 0,
          keep = true;

        // Paginate through Reddit results until we hit the cutoff date
        while (keep) {
          try {
            const data = await redditRequest(
              `https://www.reddit.com/r/${chunk}/new.json`,
              { limit: 100, after },
            );
            const children = data.data?.children || [];
            if (children.length === 0) break;

            for (const child of children) {
              const msg = Utils.extractContent(child, "post");
              // Stop if we reach a post that is older than 7 days
              if (!msg || dayjs(msg.created_utc).unix() < cutoff) {
                keep = false;
                break;
              }

              await producer.send({
                topic: "reddit.raw.posts",
                messages: [{ key: msg.event_id, value: JSON.stringify(msg) }],
              });
              after = child.data.name;
            }

            fetched += children.length;
            if (fetched > 5000) break; // Safety cap per subreddit batch

            // Report progress percentage back to the API for the UI progress bar
            const lastPostTs = children[children.length - 1].data.created_utc;
            const percentage = Math.min(
              100,
              Math.max(
                0,
                ((dayjs().unix() - lastPostTs) / (7 * 24 * 3600)) * 100,
              ),
            );
            await axios.patch(
              `${CONFIG.API_BASE_URL}/topics/${topic_id}/status`,
              { percentage },
            );

            await Utils.sleep(1500); // Respect Reddit API between pages
          } catch (e: any) {
            break;
          }
        }
      }
      // Mark task as finished
      await axios.patch(`${CONFIG.API_BASE_URL}/topics/${topic_id}/status`, {
        status: "COMPLETED",
        percentage: 100,
      });
    },
  });
}

// Kick off the services concurrently
startPolling();
startBackfill();
