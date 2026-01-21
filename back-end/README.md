# Reddit Insight – Complete Backend Technical Specification

## Document Purpose

This document is a **complete, implementation-grade technical specification** for the Reddit Insight backend system.

The document is suitable for:

* full backend implementation
* academic evaluation (TAP / distributed systems)
* architectural justification of Kafka + Spark

This specification intentionally avoids runtime LLM dependency for analytics logic.

---

## Part I – Functional Requirements

### 1. Purpose of the System

Reddit Insight is a backend-heavy, near–real-time data processing platform designed to ingest, process, and analyze **high-volume, fast-changing Reddit data**.

The system focuses on:

* topic tracking
* trend detection
* statistical and sentiment analysis

All core intelligence is **deterministic and statistical**, not AI-driven at runtime.

Apache Kafka and Apache Spark are **mandatory architectural components**, justified by:

* high ingestion rate
* stream processing requirements
* stateful windowed analytics
* horizontal scalability

---

### 2. Core Functional Features

The system delivers value via **two independent insight mechanisms**.

#### 2.1 User-Defined Topic Insights

Users define topics using **natural language descriptions**. These descriptions are converted (offline or asynchronously) into a structured topic configuration used by Spark.

**Topic Configuration Model**

* topic_id (UUID)
* description (free text)
* keyword_groups (array of arrays)
* allowed_subreddits (array)
* filters:

  * minimum score
  * event type (post/comment)
  * time windows
* update frequency
* activation status

**Backend Responsibilities**

* persist topic definitions
* version topic configurations
* distribute topic rules to Spark jobs

Runtime topic tracking is strictly **rule-based and deterministic**.

---

#### 2.2 Global Viral Topic Detection

The system continuously detects the **Top 3 viral topics globally**, regardless of user configuration.

A viral topic is defined as a **keyword or n-gram cluster** exhibiting statistically abnormal growth relative to its own historical baseline.

This feature guarantees system usefulness even with zero user-defined topics.

---

## Part II – System Architecture

### 3. High-Level Architecture

Reddit Insight follows an **event-driven, stream-first architecture**.

Core components:

1. Reddit ingestion service
2. Kafka event backbone
3. Spark Structured Streaming jobs
4. Metrics storage layer
5. REST API service
6. Optional local LLM enrichment layer

All components are **loosely coupled** and independently scalable.

---

### 4. Data Model

#### 4.1 Event Definition

Each Reddit post or comment is modeled as an **immutable event**.

```json
{
  "event_id": "uuid",
  "event_type": "post | comment",
  "subreddit": "string",
  "author": "string | null",
  "created_utc": "timestamp",
  "text": "string",
  "score": "int",
  "num_comments": "int | null",
  "ingested_at": "timestamp"
}
```

Events are append-only and never mutated after ingestion.

---

## Part III – Ingestion Layer (Kafka)

### 5. Ingestion Strategy

Reddit does not provide push-based streaming APIs. Therefore, ingestion is performed via **high-frequency polling**.

The ingestion service:

* polls Reddit APIs on fixed intervals
* normalizes responses into event schema
* publishes events to Kafka

Deduplication is handled using event_id hashing.

---

### 6. Kafka Topics

| Topic                   | Purpose                       |
| ----------------------- | ----------------------------- |
| reddit.raw.posts        | raw post events               |
| reddit.raw.comments     | raw comment events            |
| reddit.processed.events | normalized unified stream     |
| reddit.topic.matched    | events matched to user topics |
| reddit.global.trends    | global viral topic metrics    |

Kafka serves as:

* durable event log
* replay mechanism
* backpressure buffer

ZooKeeper is **not required** in modern Kafka deployments.

---

## Part IV – Stream Processing (Apache Spark)

### 7. Spark Processing Model

* Spark Structured Streaming
* Micro-batch execution
* Kafka as source and sink
* Event-time processing

Watermarking:

* 10 minutes (configurable)


- **Adaptive Windowing Strategy**:
  To handle varying data sparsity, the system concurrently calculates metrics over three window types:
  1.  **Short (30m window, 15m slide)**: Optimized for high-volume, viral topics.
  2.  **Medium (60m window, 30m slide)**: Balanced for standard queries.
  3.  **Long (120m window, 60m slide)**: Optimized for low-volume/sparse topics to ensure velocity metrics are capturable.
  
  All metrics are output with a `window_type` tag (`30m`, `60m`, `120m`).

---

### 8. Text Preprocessing

All text processing is deterministic:

* lowercase
* punctuation removal
* tokenization
* optional stopword filtering

No embeddings, transformers, or semantic inference.

---

### 9. Topic Matching Logic

Topic matching is **rule-based**.

A topic matches an event if:

* event.subreddit ∈ allowed_subreddits
* event.score ≥ threshold
* at least N keywords from the same keyword_group appear
* OR a configured n-gram is detected

This avoids single-word ambiguity.

---

### 10. Metrics Computation

Spark computes metrics per topic and per window.

Mandatory metrics:

**Mentions**

* count of matching events

**Engagement**

```
engagement = sum(score + num_comments)
```

**Velocity**

```
velocity = (current_window_mentions - previous_window_mentions) / window_duration
```

**Acceleration**

```
acceleration = velocity_current - velocity_previous
```

**Trend Score**

```
trend_score = w1 * velocity + w2 * acceleration + w3 * engagement
```

Weights (example default):

* w1 = 0.5
* w2 = 0.3
* w3 = 0.2

Weights must be configurable.

---

### 11. Global Viral Topic Detection

Spark independently processes all events:

1. extract candidate n-grams (2–4 words)
2. aggregate frequencies over short and long windows
3. compute velocity and acceleration
4. rank by trend_score
5. select top 3 per window

Results are persisted regardless of user activity.

---

### 12. Sentiment Analysis (Optional)

Sentiment uses lexicon-based methods (e.g. VADER):

* deterministic
* explainable
* low-cost

Sentiment trends are aggregated, not stored per event.

---

## Part V – Storage Layer

### 13. Storage Strategy

**Raw Events**

* Optional object storage (S3-compatible)
* Used for replay and debugging

**Aggregated Metrics**

* Relational DB or time-series DB
* Indexed by topic_id + timestamp

Spark never writes raw text to the database.

---

## Part VI – API Layer

### 14. REST API

APIs are strictly **read-only for analytics**.

Mandatory endpoints:

* POST /topics
* GET /topics/{id}
* GET /topics/{id}/report
* GET /trending/global

All responses serve **precomputed data only**.

---

## Part VII – Optional Local LLM Layer

### 15. LLM Usage Constraints

A local LLM may consume Spark outputs to:

* generate explanations
* summarize trends
* compare topics

The LLM:

* never accesses raw streams
* never influences rankings or metrics

---

## Part VIII – Non-Functional Requirements

* Horizontal scalability
* Fault tolerance
* Configurable via environment variables
* Modular codebase
* Extensive inline documentation

---

## Part IX – Design Justification

The system is precise without AI because it:

* measures behavioral change
* uses multi-word constraints
* applies temporal statistics
* filters noise through windows and baselines
