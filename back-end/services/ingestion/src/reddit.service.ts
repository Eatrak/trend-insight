import axios from "axios";
import { Utils } from "./utils.js";

/**
 * Service for interacting with the Reddit API.
 */
export class RedditService {
  /**
   * Executes a GET request to the Reddit API with rate-limit handling.
   */
  static async request(
    url: string,
    params: any,
    userAgent: string,
  ): Promise<any> {
    try {
      const response = await axios.get(url, {
        headers: { "User-Agent": userAgent },
        params,
        timeout: 20000,
      });
      return response.data;
    } catch (error: any) {
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
      throw error;
    }
  }

  /**
   * Polls Reddit for new posts and comments across a list of subreddits.
   */
  static async poll(producer: any, subs: string[], lastSeen: any, config: any) {
    const subStr = subs.join("+");
    for (const type of ["post", "comment"] as const) {
      try {
        const url = `https://www.reddit.com/r/${subStr}/${type === "post" ? "new" : "comments"}.json`;
        const data = await this.request(
          url,
          { limit: config.POST_LIMIT },
          config.USER_AGENT,
        );
        const children = data.data?.children || [];

        for (const child of children) {
          const msg = Utils.extractContent(child, type);
          if (msg && !lastSeen[type].has(msg.event_id)) {
            await producer.send({
              topic: `reddit.raw.${type}s`,
              messages: [{ key: msg.event_id, value: JSON.stringify(msg) }],
            });
            lastSeen[type].add(msg.event_id);
          }
        }
        await Utils.sleep(2000);
      } catch (e: any) {
        console.error(`[ingestion] Error polling ${type}s:`, e.message);
      }
    }
  }
}
