import express, { Request, Response } from 'express';
import Database from 'better-sqlite3';
import { Client } from '@elastic/elasticsearch';
import dotenv from 'dotenv';
import path from 'path';

dotenv.config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8000;
const TOPICS_DB_PATH = process.env.TOPICS_DB_PATH || '/data/topics.db';
const ELASTICSEARCH_URL = process.env.ELASTICSEARCH_URL || 'http://elasticsearch:9200';

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
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
`);

// ----------------------------------------------------------------------------
// Storage: Elasticsearch (Metrics, Trends)
// ----------------------------------------------------------------------------
const esClient = new Client({
  node: ELASTICSEARCH_URL,
  auth: {
    username: process.env.BASIC_AUTH_USER || 'admin',
    password: process.env.BASIC_AUTH_PASS || 'admin'
  },
  tls: { rejectUnauthorized: false }
});

// ----------------------------------------------------------------------------
// API Endpoints
// ----------------------------------------------------------------------------

// 1. POST /topics
app.post('/topics', (req: Request, res: Response) => {
  try {
    const { id, description, keywords, subreddits, filters, update_frequency, is_active } = req.body;
    
    // Basic validation
    if (!id || !keywords || !subreddits) {
       res.status(400).json({ error: 'Missing required fields: id, keywords, subreddits' });
       return;
    }

    const stmt = db.prepare(`
      INSERT INTO topics (id, description, keywords, subreddits, filters_json, update_frequency_seconds, is_active, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    stmt.run(
      id, 
      description || '', 
      Array.isArray(keywords) ? JSON.stringify(keywords) : keywords, 
      Array.isArray(subreddits) ? subreddits.join(',') : subreddits, 
      JSON.stringify(filters || {}), 
      update_frequency || 60, // Default to 60s
      is_active ? 1 : 0,
      new Date().toISOString()
    );

    res.status(201).json({ message: 'Topic created', id });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 2. GET /topics
app.get('/topics', (req: Request, res: Response) => {
  try {
    const stmt = db.prepare('SELECT * FROM topics');
    const rows = stmt.all();
    // Parse JSON fields
    const topics = rows.map((r: any) => ({
      ...r,
      keywords: isJson(r.keywords) ? JSON.parse(r.keywords) : r.keywords,
      subreddits: r.subreddits.split(','),
      filters: JSON.parse(r.filters_json || '{}')
    }));
    res.json(topics);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 3. GET /topics/{id}
app.get('/topics/:id', (req: Request, res: Response) => {
  try {
    const stmt = db.prepare('SELECT * FROM topics WHERE id = ?');
    const topic = stmt.get(req.params.id) as any;
    
    if (!topic) {
      res.status(404).json({ error: 'Topic not found' });
      return;
    }

    res.json({
      ...topic,
       keywords: isJson(topic.keywords) ? JSON.parse(topic.keywords) : topic.keywords,
       subreddits: topic.subreddits.split(','),
       filters: JSON.parse(topic.filters_json || '{}')
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

// 4. GET /topics/{id}/report
// Reads metrics from Elasticsearch (reddit-topic-metrics*)
app.get('/topics/:id/report', async (req: Request, res: Response) => {
  try {
    const topicId = req.params.id;
    // Query last 24h metrics
    const result = await esClient.search({
      index: 'reddit-topic-metrics*', // Logstash must push to this index
      body: {
        query: {
          bool: {
            must: [
              { match: { topic_id: topicId } }
            ]
          }
        },
        sort: [{ timestamp: { order: 'desc' } }],
        size: 100 // Pagination could be added
      }
    });

    const hits = result.hits.hits.map(h => h._source);
    res.json({ topic_id: topicId, metrics: hits });
  } catch (error: any) {
     console.error("ES Error:", error);
    // If index doesn't exist yet, return empty
    res.json({ topic_id: req.params.id, metrics: [] });
  }
});

// 5. GET /trending/global
// Reads global trends from Elasticsearch (reddit-global-trends*)
app.get('/trending/global', async (req: Request, res: Response) => {
  try {
    // Get latest top 3 via ES aggregation or just filtering recent records
    // Since Spark writes Top 3 explicitly, we can just fetch the latest records.
    const result = await esClient.search({
      index: 'reddit-global-trends*',
      body: {
        sort: [{ timestamp: { order: 'desc' } }],
        size: 20 
      }
    });

    const hits = result.hits.hits.map(h => h._source);
    res.json({ global_trends: hits });
  } catch (error: any) {
    console.error("ES Error:", error);
    res.json({ global_trends: [] });
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
