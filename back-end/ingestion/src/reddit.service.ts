import axios from "axios";
import dayjs from "dayjs";
import { Utils } from "./utils.js";

/**
 * Service for interacting with the Reddit API.
 * Handles rate limiting, pagination, and data ingestion logic.
 */
export class RedditService {
  /**
   * Executes a GET request to the Reddit API with comprehensive error handling.
   * Specifically handles 429 Rate Limit responses by waiting for the 'retry-after' header duration.
   */
  static async request(
    url: string,
    params: any,
    userAgent: string,
  ): Promise<any> {
    try {
      // Execute the GET request with the provided user agent and parameters
      const response = await axios.get(url, {
        headers: { "User-Agent": userAgent },
        params,
        timeout: 20000,
      });
      return response.data;
    } catch (error: any) {
      // If rate limited (429), wait for the duration specified by Reddit and retry
      if (error.response?.status === 429) {
        const retryAfter = parseInt(
          error.response.headers["retry-after"] || "5",
        );
        console.warn(
          `[ingestion] Rate limited (429). Waiting ${retryAfter}s...`,
        );

        await Utils.sleep(retryAfter * 1000);
        return this.request(url, params, userAgent);
      }

      // Propagate other errors to the caller
      throw error;
    }
  }

  /**
   * Universal ingestion method using Reddit Search API for a specific topic.
   * Fetches posts matching the topic's keywords or subreddit until:
   * 1. The cutoff timestamp is reached.
   * 2. The maximum post limit is reached.
   * 3. The API returns no more results (depth limit).
   */
  static async ingestTopicPosts(
    producer: any,
    topic: any,
    config: any,
    options: {
      cutoffTimestamp: number;
      onProgress?: (percentage: number) => Promise<void>;
      heartbeat?: () => Promise<void>;
      maxLimit: number;
    },
  ) {
    const { cutoffTimestamp, onProgress, heartbeat, maxLimit } = options;

    // construct the search query from keywords
    const query = Utils.buildQuery(topic.keywords);
    if (!query) return;

    // construct the subreddit string (limited to 30 as per API rules)
    const subreddits = topic.subreddits || [];
    const subStr = subreddits.join("+");

    // Reverted to Standard Pagination due to Reddit API timestamp limitations.
    // Iterates until maxLimit or cutoffTimestamp is reached.
    let after: string | null = null;
    let fetchedCount = 0;
    let isFinished = false;

    try {
      console.log(
        `[ingestion] Starting Standard Ingestion for ${topic.id}. MaxLimit: ${maxLimit}`,
      );

      // Main ingestion loop
      while (!isFinished && fetchedCount < maxLimit) {
        if (heartbeat) await heartbeat();

        const url = `https://www.reddit.com/r/${subStr}/search.json`;
        const params: any = {
          q: query,
          sort: "new",
          restrict_sr: "on",
          t: "all",
          limit: 100,
        };
        // Pagination token
        if (after) params.after = after;

        const data = await this.request(url, params, config.USER_AGENT);
        const posts = data.data?.children || [];

        // If no more results, stop
        if (posts.length === 0) {
          console.warn(
            `[ingestion] No more children returned (depth limit reached). Fetched: ${fetchedCount}`,
          );
          break;
        }

        for (const post of posts) {
          const normalizedPost = Utils.extractContent(post);
          if (!normalizedPost) continue;

          const createdUnix = normalizedPost.created_utc;

          // Stop if we reached beyond the cutoff timestamp (older posts)
          if (createdUnix < cutoffTimestamp) {
            isFinished = true;
            break;
          }

          // Calculate the engagement of the post
          const engagement =
            (normalizedPost.score || 0) +
            (normalizedPost.num_comments || 0) +
            1;

          // Send to Kafka
          await producer.send({
            topic: "reddit.topic.matches",
            messages: [
              {
                key: normalizedPost.event_id,
                value: JSON.stringify({
                  ...normalizedPost,
                  topic_id: topic.id,
                  timestamp: normalizedPost.created_utc,
                  engagement,
                  event_type: "matched_post",
                }),
              },
            ],
          });
        }

        fetchedCount += posts.length;
        const lastChild = posts[posts.length - 1];
        after = lastChild.data.name;

        // Progress Calculation and Reporting
        if (onProgress && posts.length > 0) {
          const lastCreated = lastChild.data.created_utc;
          const now = dayjs().unix();
          const totalRange = now - cutoffTimestamp;
          const elapsedRange = now - lastCreated;
          const progress = Utils.clamp(
            (elapsedRange / totalRange) * 100,
            0,
            100,
          );
          await onProgress(progress);
        }

        // Respect rate limits (1 request per second logic)
        await Utils.sleep(1000);
      }

      console.log(
        `[ingestion] Finished ingestion for ${topic.id}. Total fetched: ${fetchedCount}`,
      );
    } catch (e: any) {
      console.error(
        `[ingestion] Ingest error for topic ${topic.id}:`,
        e.message,
      );
      throw e;
    }
  }
}
