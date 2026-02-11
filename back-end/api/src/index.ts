import OpenAI from "openai";
import express, { Request, Response } from "express";
import mysql from "mysql2/promise";
import { Client } from "@elastic/elasticsearch";
import dotenv from "dotenv";
import { Kafka } from "kafkajs";

dotenv.config();

const app = express();
app.use(express.json());

// Request logger middleware
app.use((req, res, next) => {
  console.log(`[API] ${req.method} ${req.url}`);
  next();
});

const PORT = process.env.PORT || 8000;
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
// Storage: MySQL (Topics)
// ----------------------------------------------------------------------------
const db = mysql.createPool({
  host: process.env.MYSQL_HOST || "trend-mysql",
  user: process.env.MYSQL_USER || "trend_user",
  password: process.env.MYSQL_PASSWORD || "trend_password",
  database: process.env.MYSQL_DATABASE || "trend_insight",
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
  while (true) {
    try {
      await producer.connect();
      console.log("[API] Kafka Producer connected");
      return;
    } catch (error) {
      console.error(
        "[API] Kafka Producer connection failed, retrying...",
        error,
      );

      await new Promise((res) => setTimeout(res, 5000));
    }
  }
};

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
app.get("/topics", async (req: Request, res: Response) => {
  try {
    const [rows] = await db.query("SELECT * FROM topics");
    const topics = (rows as any[]).map((r: any) => ({
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
app.post("/topics", async (req: Request, res: Response) => {
  try {
    const { id, description, keywords, subreddits, filters, update_frequency } =
      req.body;

    // Basic validation
    if (!id || !keywords || !subreddits) {
      res
        .status(400)
        .json({ error: "Missing required fields: id, keywords, subreddits" });
      return;
    }

    const sql = `
      INSERT INTO topics (id, description, keywords, subreddits, filters_json, update_frequency_seconds, created_at, backfill_status)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'IDLE')
    `;

    await db.execute(sql, [
      id,
      description || "",
      Array.isArray(keywords) ? JSON.stringify(keywords) : keywords,
      Array.isArray(subreddits) ? subreddits.join(",") : subreddits,
      JSON.stringify(filters || {}),
      update_frequency || 60, // Default to 60s
      new Date().toISOString().slice(0, 19).replace("T", " "), // MySQL Format
    ]);

    res.status(201).json({ message: "Topic created", id });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 3. GET /topics/{id}
app.get("/topics/:id", async (req: Request, res: Response) => {
  try {
    const [rows] = await db.execute("SELECT * FROM topics WHERE id = ?", [
      req.params.id,
    ]);
    const topic = (rows as any[])[0];

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
    const [rows] = await db.execute("SELECT * FROM topics WHERE id = ?", [id]);
    const topic = (rows as any[])[0];
    if (!topic) {
      res.status(404).json({ error: "Topic not found" });
      return;
    }

    // 2. Update Status -> PENDING
    await db.execute(
      "UPDATE topics SET backfill_status = 'PENDING' WHERE id = ?",
      [id],
    );

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
      topic: "topic.backfilling.tasks",
      messages: [{ value: JSON.stringify(message) }],
    });

    res.json({ message: "Backfill task queued", status: "PENDING" });
  } catch (error: any) {
    console.error("Backfill Error:", error);
    res.status(500).json({ error: error.message });
  }
});

// 9. PATCH /topics/:id/status
app.patch("/topics/:id/status", async (req: Request, res: Response) => {
  try {
    const { status, percentage } = req.body;
    const id = req.params.id;

    if (status) {
      await db.execute("UPDATE topics SET backfill_status = ? WHERE id = ?", [
        status,
        id,
      ]);
    }

    if (percentage !== undefined) {
      await db.execute(
        "UPDATE topics SET backfill_percentage = ? WHERE id = ?",
        [percentage, id],
      );
    }

    res.json({ success: true });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 4. GET /topics/:id/report
app.get("/topics/:id/report", async (req: Request, res: Response) => {
  try {
    const topicId = req.params.id;

    // AGGREGATION QUERY (Advanced Windowed Analytics)
    const result = await esClient.search({
      index: "topic-enriched-matched-posts",
      size: 0,
      query: {
        bool: {
          must: [{ term: { "topic_id.keyword": topicId } }],
        },
      },
      aggs: {
        daily_buckets: {
          date_histogram: {
            field: "@timestamp",
            calendar_interval: "1d",
            order: { _key: "asc" },
            min_doc_count: 0,
            extended_bounds: {
              min: "now-30d/d",
              max: "now/d",
            },
          },
          aggs: {
            mentions: { value_count: { field: "topic_id.keyword" } },
            engagement: { sum: { field: "engagement" } },
            sentiment_sum: { sum: { field: "sentiment_score" } },
            avg_sentiment: { avg: { field: "sentiment_score" } },
            // Moving Windows (Pipeline Aggregations)
            moving_mentions_7d: {
              moving_fn: {
                buckets_path: "mentions",
                window: 7,
                script: "MovingFunctions.sum(values)",
              },
            },
            moving_engagement_7d: {
              moving_fn: {
                buckets_path: "engagement",
                window: 7,
                script: "MovingFunctions.sum(values)",
              },
            },
            moving_sentiment_7d: {
              moving_fn: {
                buckets_path: "sentiment_sum",
                window: 7,
                script: "MovingFunctions.sum(values)",
              },
            },
            moving_mentions_30d: {
              moving_fn: {
                buckets_path: "mentions",
                window: 30,
                script: "MovingFunctions.sum(values)",
              },
            },
            moving_engagement_30d: {
              moving_fn: {
                buckets_path: "engagement",
                window: 30,
                script: "MovingFunctions.sum(values)",
              },
            },
            moving_sentiment_30d: {
              moving_fn: {
                buckets_path: "sentiment_sum",
                window: 30,
                script: "MovingFunctions.sum(values)",
              },
            },
          },
        },
      },
    });

    const metrics: any[] = [];
    const buckets = (result.aggregations as any)?.daily_buckets?.buckets || [];

    // Track previous values for growth calculation
    const prevMap: Record<string, number> = { "1d": 0, "1w": 0, "1m": 0 };

    for (const bucket of buckets) {
      const timestamp = bucket.key_as_string;
      const endTimestamp = new Date(bucket.key + 86400000).toISOString();

      const processWindow = (
        type: string,
        mentions: number,
        engagement: number,
        sentimentSum: number,
      ) => {
        const prevMentions = prevMap[type] || 0;
        const growth = prevMentions === 0 ? 1.0 : mentions / prevMentions;
        const sentiment = mentions > 0 ? sentimentSum / mentions : 0;

        metrics.push({
          topic_id: topicId,
          start:
            type === "1d"
              ? timestamp
              : new Date(
                  bucket.key - (type === "1w" ? 6 : 29) * 86400000,
                ).toISOString(),
          end: endTimestamp,
          mentions,
          engagement,
          sentiment,
          growth,
          trend_score: engagement * growth,
          window_type: type,
        });

        prevMap[type] = mentions;
      };

      // 1. Daily
      processWindow(
        "1d",
        bucket.doc_count,
        bucket.engagement?.value || 0,
        bucket.sentiment_sum?.value || 0,
      );

      // 2. Weekly
      processWindow(
        "1w",
        bucket.moving_mentions_7d?.value || 0,
        bucket.moving_engagement_7d?.value || 0,
        bucket.moving_sentiment_7d?.value || 0,
      );

      // 3. Monthly
      processWindow(
        "1m",
        bucket.moving_mentions_30d?.value || 0,
        bucket.moving_engagement_30d?.value || 0,
        bucket.moving_sentiment_30d?.value || 0,
      );
    }

    // Sort descending by end time for the report
    metrics.sort(
      (a, b) => new Date(b.end).getTime() - new Date(a.end).getTime(),
    );

    res.json({ topic_id: topicId, metrics });
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

async function initDB() {
  // Retry Logic for MySQL
  while (true) {
    try {
      // Initialize DB Schema (Create Table)
      await db.query(`
        CREATE TABLE IF NOT EXISTS topics (
          id VARCHAR(255) PRIMARY KEY,
          description TEXT,
          keywords TEXT, -- JSON array
          subreddits TEXT, -- CSV
          filters_json TEXT, -- JSON string
          update_frequency_seconds INT DEFAULT 60,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          backfill_status VARCHAR(50) DEFAULT 'IDLE', -- IDLE, PENDING, COMPLETED, ERROR
          backfill_percentage FLOAT DEFAULT 0.0
        )
      `);
      console.log("[API] MySQL DB Initialized");
      break; // Success
    } catch (e: any) {
      console.error(`[API] MySQL connection failed. Retrying...`, e.message);
      await new Promise((res) => setTimeout(res, 5000));
    }
  }
}

// ----------------------------------------------------------------------------
// Start Server
// ----------------------------------------------------------------------------
const startServer = async () => {
  try {
    console.log("[API] Starting server...");

    // Initialize DB
    await initDB();

    // Start Kafka Producer
    await connectProducer();

    app.listen(PORT, () => {
      console.log(`[API] Listening on ${PORT}`);
    });
  } catch (e) {
    console.error("[API] Failed to start server:", e);
    process.exit(1);
  }
};

startServer();
