# Reddit Insight – Usage & Testing Guide

This document explains how to run, configure, and verify the Reddit Insight backend.

## 1. Prerequisites

*   **Docker Desktop** (running)
*   `curl` (or Postman) for API testing
*   `jq` (optional, for pretty-printing JSON)

## 2. Startup

Start the entire stack (Kafka, Spark, Elasticsearch, API, Ingestion):

```bash
docker compose up --build
```

**Wait time:** Please wait **~2-3 minutes** after startup.
*   Kafka needs to elect a controller.
*   Spark needs to allocate workers and start the streaming job.
*   Elasticsearch needs to become green.

---

## 3. Verify System Health

Before testing logic, ensure infrastructure is running:

| Service | URL | Check |
| :--- | :--- | :--- |
| **Kafka UI** | [http://localhost:8085](http://localhost:8085) | Brokers should be active. |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | `reddit_streaming.py` app should be `RUNNING`. |
| **API** | [http://localhost:8000/topics](http://localhost:8000/topics) | Should return `[]` (empty list). |

---

## 4. Testing Workflow

### Step 1: Create a Topic

Tell the system to track a specific topic.

```bash
curl -X POST http://localhost:8000/topics \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ai-news",
    "description": "Artificial Intelligence News",
    "keywords": ["ai", "artificial intelligence", "llm", "chatgpt"],
    "subreddits": ["technology", "artificial", "machinelearning"],
    "is_active": true
  }'
```

*Expected Response:* `{"message":"Topic created","id":"ai-news"}`

### Step 2: Verify Ingestion (Kafka)

Open **Kafka UI** ([http://localhost:8085](http://localhost:8085)) and check topics:
1.  `reddit.raw.posts`: Should show incoming JSON events from the polling service.
2.  `reddit.topic.matched`: Should start showing events that matched your "ai" keywords.

**Note:** The ingestion service polls strictly defined subreddits in `docker-compose.yml` (`REDDIT_SUBREDDITS`).

### Step 3: Wait for Windowing (Important)

The Spark job calculates **Velocity** and **Acceleration** by comparing the *current 15m window* with the *previous 15m window*.

*   **First 0-15 mins**: Data is accumulating.
*   **15-20 mins**: First window completes, but no "previous" window exists for comparison (Velocity = N/A or 0).
*   **20+ mins**: Metrics for Velocity and Acceleration will appear as streams overlap.

### Step 4: Check Metrics API

Retrieve the computed analytics for your topic:

```bash
curl http://localhost:8000/topics/ai-news/report
```

*Expected Output (Example):*
```json
{
  "topic_id": "ai-news",
  "metrics": [
    {
      "timestamp": "2026-01-21T14:30:00Z",
      "mentions": 42,
      "engagement": 150,
      "velocity": 5.2,
      "acceleration": 1.1,
      "trend_score": 3.4
    }
  ]
}
```

### Step 5: Check Global Viral Trends

This works even if you didn't create a topic. It detects high-growth N-grams globally.

```bash
curl http://localhost:8000/trending/global
```

*Expected Output:*
```json
{
  "global_trends": [
    {
      "ngram": "new release",
      "trend_score": 8.5,
      "velocity": 12.0
    },
    {
      "ngram": "breaking news",
      "trend_score": 6.2,
      "velocity": 4.1
    }
  ]
}
```

---

## 5. Troubleshooting

**No data in `reddit.topic.matched`?**
*   Ensure the `ingestion` service is running (`docker compose ps`).
*   Check logs: `docker compose logs -f ingestion`.
*   Ensure the subreddits being polled actually contain your keywords.

**Spark job failed?**
*   Check logs: `docker compose logs -f spark-streaming`.
*   Ensure Kafka was ready before Spark tried to connect. (Restart spark-streaming container if needed: `docker compose restart spark-streaming`).

**API returns empty lists?**
*   Elasticsearch might still be indexing. Check `docker compose logs -f logstash`.
*   Ensure `logstash` is successfully connecting to Kafka.
