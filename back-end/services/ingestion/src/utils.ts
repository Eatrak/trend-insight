import dayjs from 'dayjs';

/**
 * Shared utility methods for the ingestion service.
 */
export class Utils {
    /**
     * Pauses the execution for a given number of milliseconds.
     */
    /**
     * Helper to pause execution. Used for rate limiting and connection retries.
     */
    static sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

    /**
     * Normalizes Reddit API data (posts/comments) into our unified schema.
     * Ensures consistent fields for downstream processing (Spark/Frontend).
     */
    static extractContent(child: any, eventType: 'post' | 'comment'): any {
        const d = child.data || {};
        if (!d.id) return null;

        return {
            event_id: d.name || d.id,
            event_type: eventType,
            subreddit: d.subreddit || '',
            author: d.author || null,
            created_utc: dayjs.unix(parseFloat(d.created_utc || dayjs().unix().toString())).toISOString(),
            score: parseInt(d.score || '0'),
            ingested_at: dayjs().toISOString(),
            text: eventType === 'post' ? `${d.title || ''}\n${d.selftext || ''}` : d.body || '',
            num_comments: eventType === 'post' ? parseInt(d.num_comments || '0') : 0,
        };
    }
}
