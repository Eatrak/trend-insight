import OpenAI from "openai";
import express, { Request, Response } from "express";
import Database from "better-sqlite3";
import { Client } from "@elastic/elasticsearch";
import dotenv from "dotenv";
import { Kafka } from "kafkajs";
import path from "path";

dotenv.config();

const app = express();
app.use(express.json());

// Request logger middleware
app.use((req, res, next) => {
  console.log(`[API] ${req.method} ${req.url}`);
  next();
});

const PORT = process.env.PORT || 8000;
const TOPICS_DB_PATH = process.env.TOPICS_DB_PATH || "/data/topics.db";
const ELASTICSEARCH_URL =
  process.env.ELASTICSEARCH_URL || "http://elasticsearch:9200";

// ----------------------------------------------------------------------------
// OpenAI Client
// ----------------------------------------------------------------------------
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const DEFAULT_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";

// ----------------------------------------------------------------------------
// Storage: SQLite (Topics)
// ----------------------------------------------------------------------------
const db = new Database(TOPICS_DB_PATH);

// Initialize DB Schema
db.exec(`
  CREATE TABLE IF NOT EXISTS topics (
    id TEXT PRIMARY KEY,
    description TEXT,
    keywords TEXT, -- JSON array or CSV
    subreddits TEXT, -- CSV
    filters_json TEXT, -- JSON string
    update_frequency_seconds INTEGER DEFAULT 60,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    backfill_status TEXT DEFAULT 'IDLE', -- IDLE, PENDING, COMPLETED, ERROR
    backfill_percentage REAL DEFAULT 0.0
  );
`);

// Migration: Add backfill_status if missing (for existing DBs)
try {
  db.exec("ALTER TABLE topics ADD COLUMN backfill_status TEXT DEFAULT 'IDLE'");
} catch (e: any) {
  // Ignore
}

// Migration: Add backfill_percentage if missing
try {
  db.exec("ALTER TABLE topics ADD COLUMN backfill_percentage REAL DEFAULT 0.0");
} catch (e: any) {
  // Ignore
}

// ... (Rest of code)

// 9. PATCH /topics/:id/status
app.patch("/topics/:id/status", (req: Request, res: Response) => {
  try {
    const { status, percentage } = req.body;
    const id = req.params.id;

    if (status) {
      db.prepare("UPDATE topics SET backfill_status = ? WHERE id = ?").run(
        status,
        id,
      );
    }

    if (percentage !== undefined) {
      db.prepare("UPDATE topics SET backfill_percentage = ? WHERE id = ?").run(
        percentage,
        id,
      );
    }

    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// ----------------------------------------------------------------------------
// Kafka Producer (for backfill tasks)
// ----------------------------------------------------------------------------
const kafka = new Kafka({
  clientId: "api-service",
  brokers: (process.env.KAFKA_BOOTSTRAP_SERVERS || "kafka:9092").split(","),
});
const producer = kafka.producer();

const connectProducer = async () => {
  let retries = 5;
  while (retries > 0) {
    try {
      await producer.connect();
      console.log("Kafka Producer connected");
      return;
    } catch (error) {
      console.error("Kafka Producer connection failed, retrying...", error);
      retries--;
      await new Promise((res) => setTimeout(res, 5000));
    }
  }
  console.error("Could not connect to Kafka after multiple retries.");
};

connectProducer();

// ----------------------------------------------------------------------------
// Auto-Sync Scheduler (Every 2 Minutes)
// ----------------------------------------------------------------------------
async function startAutoBackfillScheduler() {
  console.log("[Auto-Sync] Scheduler started (Interval: 2 minutes)");

  // Initial delay to let everything settle
  await new Promise((resolve) => setTimeout(resolve, 30000));

  setInterval(
    async () => {
      try {
        console.log("[Auto-Sync] Triggering auto-sync for active topics...");
        // 1. Get all active topics
        const stmt = db.prepare("SELECT * FROM topics WHERE is_active = 1");
        const topics = stmt.all() as any[];

        if (topics.length === 0) {
          console.log("[Auto-Sync] No active topics found.");
          return;
        }

        const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "")
          .split(",")
          .map((s) => s.trim())
          .filter((s) => s.length > 0);

        // 2. Queue backfill task for each topic
        for (const topic of topics) {
          // Skip if manual backfill is currently running to avoid conflicts
          if (topic.backfill_status === "PENDING") {
            console.log(
              `[Auto-Sync] Skipping ${topic.id} (Manual Backfill in progress)`,
            );
            continue;
          }

          const message = {
            topic_id: topic.id,
            subreddits: allowedSubreddits,
            lookback_seconds: 48 * 60 * 60, // 48 Hours
          };

          // We do NOT update the DB status to 'PENDING' here to avoid blocking the UI status.
          // The ingestion service will process it purely in the background.
          // However, the ingestion service DOES update status to PENDING/COMPLETED.
          // To avoid UI flicker, we might want to check how ingestion handles it.
          // Looking at ingestion service:
          // callback: await axios.patch(... status: "COMPLETED" ...)
          // This effectively "resets" the status.
          // If the user triggers manual backfill, it sets PENDING.
          // If auto-sync runs, it sends a task. Ingestion processes it.
          // Ingestion updates status to PENDING (step 2? no request from api does that).
          // Wait, ingestion service DOES send patch updates for progress.
          // This might interfere with UI if the user thinks it's "idle".
          // But the user requested "auto sync".
          // Let's proceed: The ingestion updates will happen.
          // Ideally we might want a separate "auto-sync" status or just let it be.

          await producer.send({
            topic: "reddit.tasks.backfill",
            messages: [{ value: JSON.stringify(message) }],
          });

          console.log(`[Auto-Sync] Queued sync for ${topic.id}`);
        }
      } catch (error: any) {
        console.error("[Auto-Sync] Error:", error.message);
      }
    },
    2 * 60 * 1000,
  ); // 2 Minutes
}

startAutoBackfillScheduler();

// ----------------------------------------------------------------------------
// Storage: Elasticsearch (Metrics, Trends)
// ----------------------------------------------------------------------------
const esClient = new Client({
  node: ELASTICSEARCH_URL,
  auth: {
    username: process.env.BASIC_AUTH_USER || "admin",
    password: process.env.BASIC_AUTH_PASS || "admin",
  },
  tls: { rejectUnauthorized: false },
});

// ----------------------------------------------------------------------------
// API Endpoints
// ----------------------------------------------------------------------------

app.get("/subreddits", (req: Request, res: Response) => {
  const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "")
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  res.json({ subreddits: allowedSubreddits });
});

// 2. GET /topics
app.get("/topics", (req: Request, res: Response) => {
  try {
    const stmt = db.prepare("SELECT * FROM topics");
    const rows = stmt.all();
    // Parse JSON fields
    const topics = rows.map((r: any) => ({
      ...r,
      keywords: isJson(r.keywords) ? JSON.parse(r.keywords) : r.keywords,
      subreddits: (r.subreddits || "").split(","),
      filters: JSON.parse(r.filters_json || "{}"),
    }));
    res.json(topics);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 1. POST /topics
app.post("/topics", (req: Request, res: Response) => {
  try {
    const {
      id,
      description,
      keywords,
      subreddits,
      filters,
      update_frequency,
      is_active,
    } = req.body;

    // Basic validation
    if (!id || !keywords || !subreddits) {
      res
        .status(400)
        .json({ error: "Missing required fields: id, keywords, subreddits" });
      return;
    }

    const stmt = db.prepare(`
      INSERT INTO topics (id, description, keywords, subreddits, filters_json, update_frequency_seconds, is_active, created_at, backfill_status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'IDLE')
    `);

    stmt.run(
      id,
      description || "",
      Array.isArray(keywords) ? JSON.stringify(keywords) : keywords,
      Array.isArray(subreddits) ? subreddits.join(",") : subreddits,
      JSON.stringify(filters || {}),
      update_frequency || 60, // Default to 60s
      is_active ? 1 : 0,
      new Date().toISOString(),
    );

    res.status(201).json({ message: "Topic created", id });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 3. GET /topics/{id}
app.get("/topics/:id", (req: Request, res: Response) => {
  try {
    const stmt = db.prepare("SELECT * FROM topics WHERE id = ?");
    const topic = stmt.get(req.params.id) as any;

    if (!topic) {
      res.status(404).json({ error: "Topic not found" });
      return;
    }

    res.json({
      ...topic,
      keywords: isJson(topic.keywords)
        ? JSON.parse(topic.keywords)
        : topic.keywords,
      subreddits: (topic.subreddits || "").split(","),
      filters: JSON.parse(topic.filters_json || "{}"),
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 8. POST /topics/:id/backfill
app.post("/topics/:id/backfill", async (req: Request, res: Response) => {
  try {
    const id = req.params.id;
    // 1. Get Topic
    const stmt = db.prepare("SELECT * FROM topics WHERE id = ?");
    const topic = stmt.get(id) as any;
    if (!topic) {
      res.status(404).json({ error: "Topic not found" });
      return;
    }

    // 2. Update Status -> PENDING
    db.prepare(
      "UPDATE topics SET backfill_status = 'PENDING' WHERE id = ?",
    ).run(id);

    // 3. Send Task to Kafka (Global Scan)
    // We send all allowed subreddits so that the backfill matches the real-time monitoring scope.
    const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "")
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    const message = {
      topic_id: id,
      subreddits: allowedSubreddits,
    };

    await producer.send({
      topic: "reddit.tasks.backfill",
      messages: [{ value: JSON.stringify(message) }],
    });

    res.json({ message: "Backfill task queued", status: "PENDING" });
  } catch (error: any) {
    console.error("Backfill Error:", error);
    res.status(500).json({ error: error.message });
  }
});

// 9. PATCH /topics/:id/status
app.patch("/topics/:id/status", (req: Request, res: Response) => {
  try {
    const { status, percentage } = req.body;
    const id = req.params.id;

    if (status) {
      db.prepare("UPDATE topics SET backfill_status = ? WHERE id = ?").run(
        status,
        id,
      );
    }

    if (percentage !== undefined) {
      db.prepare("UPDATE topics SET backfill_percentage = ? WHERE id = ?").run(
        percentage,
        id,
      );
    }

    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});
// 4. GET /topics/{id}/report
// 4. GET /topics/:id/report
// Reads metrics from Elasticsearch (reddit-topic-metrics*)
app.get("/topics/:id/report", async (req: Request, res: Response) => {
  try {
    const topicId = req.params.id;

    // AGGREGATION QUERY (ES Upsert Architecture)
    // We query the granular index where each document is a unique post (deduplicated by upsert).
    // We simply sum the engagement stats for each day.
    const result = await esClient.search({
      index: "reddit-topic-granular", // New index
      size: 0, // No hits, just aggs
      query: {
        bool: {
          must: [{ term: { "topic_id.keyword": topicId } }],
        },
      },
      aggs: {
        daily_buckets: {
          date_histogram: {
            field: "@timestamp", // Logstash maps created_utc/timestamp to @timestamp
            calendar_interval: "1d",
            order: { _key: "asc" },
            min_doc_count: 0, // Ensure we see empty days if we want continuous lines? Actually 0 is safer to skip for sparse data
            extended_bounds: {
              min: "now-30d/d",
              max: "now/d",
            },
          },
          aggs: {
            total_engagement: { sum: { field: "engagement" } }, // Sum of (score + comments + 1)
            avg_sentiment: { avg: { field: "sentiment_score" } },
          },
        },
      },
    });

    const dailyMetrics: any[] = [];
    const buckets = (result.aggregations as any)?.daily_buckets?.buckets || [];

    for (const bucket of buckets) {
      // Convert bucket to Metric format
      dailyMetrics.push({
        topic_id: topicId,
        start: bucket.key_as_string,
        end: new Date(bucket.key + 86400000).toISOString(), // Approx end of day
        mentions: bucket.doc_count,
        engagement: bucket.total_engagement.value,
        sentiment: bucket.avg_sentiment.value || 0,
        window_type: "1d",
      });
    }

    // --- Synthesis: 1d -> 1w, 1m ---
    const allWindows = aggregateDailyToWindows(dailyMetrics);

    // --- Metric Enrichment (Compute-on-Read) ---
    const finalMetrics = enrichMetrics(allWindows);

    // Sort descending by end time for the report
    finalMetrics.sort(
      (a, b) => new Date(b.end).getTime() - new Date(a.end).getTime(),
    );

    res.json({ topic_id: topicId, metrics: finalMetrics });
  } catch (error: any) {
    console.error("ES Error:", error);
    if (error.meta && error.meta.body) {
      console.error(
        "ES Error Details:",
        JSON.stringify(error.meta.body, null, 2),
      );
    }
    // If index doesn't exist yet, return empty
    res.json({ topic_id: req.params.id, metrics: [] });
  }
});

// Helper: Synthesize 7d and 30d views from daily metrics
function aggregateDailyToWindows(dailyRecords: any[]): any[] {
  // 1. Ensure sorted by date ASC
  const sorted = [...dailyRecords].sort(
    (a, b) => new Date(a.start).getTime() - new Date(b.start).getTime(),
  );
  const result: any[] = [];

  // Helper to sum previous N days
  const getWindowSum = (index: number, days: number, label: string) => {
    let mentions = 0;
    let engagement = 0;
    const currentEnd = new Date(sorted[index].end);
    // Start date of the window is (end - days)
    const windowStart = new Date(
      currentEnd.getTime() - days * 24 * 60 * 60 * 1000,
    );

    // Scan backwards for records that fall within [windowStart, currentEnd]
    let totalSentimentWeighted = 0;
    let totalMentionsForSentiment = 0;

    for (let j = index; j >= 0; j--) {
      const recStart = new Date(sorted[j].start);
      if (recStart >= windowStart) {
        const m = sorted[j].mentions || 0;
        mentions += m;
        engagement += sorted[j].engagement || 0;
        totalSentimentWeighted += (sorted[j].sentiment || 0) * m;
        totalMentionsForSentiment += m;
      } else {
        break; // Out of range
      }
    }

    const weightedSentiment =
      totalMentionsForSentiment > 0
        ? totalSentimentWeighted / totalMentionsForSentiment
        : 0;

    return {
      ...sorted[index],
      window_type: label,
      mentions,
      engagement,
      sentiment: weightedSentiment,
      start: windowStart.toISOString(),
      // end remains the same as daily record
    };
  };

  for (let i = 0; i < sorted.length; i++) {
    // Add original 1d record
    result.push({ ...sorted[i], window_type: "1d" });

    // Synthesize 1w (7 days)
    result.push(getWindowSum(i, 7, "1w"));

    // Synthesize 1m (30 days)
    result.push(getWindowSum(i, 30, "1m"));
  }

  return result;
}

// Helper: Compute Velocity & Acceleration
function enrichMetrics(rawDocs: any[]) {
  // 1. Group by window_type (30m, 60m, 120m)
  const groups: Record<string, any[]> = {};
  rawDocs.forEach((d) => {
    const type = d.window_type || "1d";
    if (!groups[type]) groups[type] = [];
    groups[type].push(d);
  });

  const output: any[] = [];
  // 2. Process each group
  for (const type of Object.keys(groups)) {
    // Sort by start time ASC to find previous window
    const docs = groups[type].sort(
      (a, b) => new Date(a.start).getTime() - new Date(b.start).getTime(),
    );

    for (let i = 0; i < docs.length; i++) {
      const curr = docs[i];
      const prev = docs[i - 1]; // Simple predecessor check

      let growth = 1.0;

      // Check if prev is valid predecessor
      if (prev) {
        const m_curr = curr.mentions || 0;
        const m_prev = prev.mentions || 0;
        // Avoid division by zero: treat 0 as 1 for baseline comparison
        const divisor = m_prev === 0 ? 1 : m_prev;
        growth = m_curr / divisor;
      }

      // Simplified score: Engagement + (Growth * 10)
      // If growth is 2x, that adds 20 points.
      // If engagement is 1000, that dominates.
      // Let's just sum them roughly for now or likely the user doesn't strictly rely on this computed trend_score yet.
      const trend_score = (curr.engagement || 0) * growth;

      output.push({
        ...curr,
        growth,
        trend_score,
      });
    }
  }

  return output;
}

// 6. POST /generate-config
app.post("/generate-config", async (req: Request, res: Response) => {
  try {
    const { description } = req.body;
    if (!description) {
      res.status(400).json({ error: "Description is required" });
      return;
    }

    if (!process.env.OPENAI_API_KEY) {
      res.status(500).json({ error: "OPENAI_API_KEY is not configured" });
      return;
    }

    const allowedSubreddits = (process.env.REDDIT_SUBREDDITS || "")
      .split(",")
      .map((s) => s.trim());
    const allowedListString = allowedSubreddits.join(", ");

    const completion = await openai.chat.completions.create({
      model: DEFAULT_MODEL,
      messages: [
        {
          role: "system",
          content: `You are a configuration generator for a trend monitoring tool.
          Based on the user description, extract a JSON object with:
          - id: a strictly kebab-case identifier (max 30 chars).
          - keywords: A LIST OF LISTS of strings (CNF Logic).
             - Outer List = AND (All groups must match)
             - Inner List = OR (At least one term in the group must match)
             - Example: [["rust", "rs"], ["job", "hiring"]] -> (rust OR rs) AND (job OR hiring).
          - subreddits: array of 3-5 relevant subreddits selected ONLY from the provided allowed list.
          - description: a polished version of the user's description.

          ALLOWED SUBREDDITS: [${allowedListString}]

          IMPORTANT: You must ONLY choose subreddits from the ALLOWED SUBREDDITS list. Do NOT invent new ones.`,
        },
        {
          role: "user",
          content: description,
        },
      ],
      response_format: { type: "json_object" },
    });

    const responseContent = completion.choices[0].message?.content;
    if (!responseContent) throw new Error("Empty response from OpenAI");

    let config = JSON.parse(responseContent);

    // FALLBACK: Ensure ID is kebab-case and not generic
    if (
      !config.id ||
      config.id.includes("topic-id") ||
      config.id.match(/[A-Z\s]/)
    ) {
      const source = (config.keywords && config.keywords[0]) || description;
      config.id = source
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "")
        .slice(0, 30);
    }

    // SANITIZER: Ensure subreddits is array of strings
    if (config.subreddits && Array.isArray(config.subreddits)) {
      config.subreddits = config.subreddits.map((s: any) =>
        typeof s === "string" ? s : s.name || s.id || JSON.stringify(s),
      );
    }

    res.json(config);
  } catch (error: any) {
    console.error("OpenAI Error:", error);
    res
      .status(500)
      .json({ error: error.message || "Failed to generate config" });
  }
});

// 7. POST /generate-random-prompt
app.post("/generate-random-prompt", async (req: Request, res: Response) => {
  try {
    if (!process.env.OPENAI_API_KEY) {
      res.status(500).json({ error: "OPENAI_API_KEY is not configured" });
      return;
    }

    const categories = [
      "Technology & Gadgets",
      "Gaming & Esports",
      "Movies & TV Shows",
      "Cryptocurrency & Finance",
      "Health & Fitness",
      "Travel & Digital Nomad",
      "Programming & AI",
      "Politics & World News",
      "Home Improvement & DIY",
      "Music & Concerts",
    ];
    const randomCategory =
      categories[Math.floor(Math.random() * categories.length)];

    const completion = await openai.chat.completions.create({
      model: DEFAULT_MODEL,
      messages: [
        {
          role: "system",
          content: `You are an assistant for a Trend Intelligence platform. 
          Generate a SINGLE, short user intent (max 15 words) that focuses on **analyzing trends**, **tracking volume**, or **monitoring sentiment shifts** for a topic in the given category.
          
          NEGATIVE CONSTRAINTS: 
          - Do NOT use passive phrases like "stay updated", "keep up with", "learn about", or "follow". 
          - Do NOT mention specific subreddits or "reddit.com".
          
          Output ONLY the sentence.`,
        },
        {
          role: "user",
          content: `Category: ${randomCategory}
          
          Target Style Examples:
          - "Analyze the spike in negative sentiment around the new iPhone."
          - "Track the discussion volume regarding the upcoming election."
          - "Monitor the trend trajectory of sustainable fashion brands."`,
        },
      ],
    });

    const result = completion.choices[0].message?.content?.trim() || "";

    const cleaned = result
      .replace(/^(Sentence|Output|Response|Prompt|Here is a sentence):\s*/i, "")
      .replace(/^["']|["']$/g, "")
      .trim();

    res.json({ prompt: cleaned });
  } catch (error: any) {
    console.error("OpenAI Error:", error);
    res
      .status(500)
      .json({ error: error.message || "Failed to generate prompt" });
  }
});

function isJson(str: string) {
  try {
    JSON.parse(str);
  } catch (e) {
    return false;
  }
  return true;
}

app.listen(PORT, () => {
  console.log(`[API] Listening on ${PORT}`);
  console.log(`[API] DB Path: ${TOPICS_DB_PATH}`);
});
