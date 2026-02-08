# Trend Insight

A high-performance, real-time data pipeline for tracking and analyzing Reddit trends. This system ingests targeted data from Reddit, processes it using Spark Structured Streaming for trend analysis, and visualizes insights via an API and Elasticsearch.

![Trend Insight App](./images/app.png)

## 🌟 Why Trend Insight?

In a world where social media trends move at light speed, understanding what is being discussed and how fast it's growing is critical.

### ⚡️ Targeted Ingestion

Trend Insight intelligently polls Reddit for specific topics and keywords, ensuring you capture exactly what matters.

### ⏳ Instant Historical Backfill

The system features a **dual-mode ingestion engine** that allows you to instantly backfill historical data (up to ~1,000 posts or ~month history) for any new topic, providing immediate context before real-time tracking begins.

### 📈 Advanced Metrics

The system calculates key metrics for each topic over time:

- **Volume**: Number of related posts.
- **Engagement**: Total impact (upvotes + comments).
- **Growth Rate**: Velocity of the trend.
- **Sentiment**: (Coming Soon) Analysis of post sentiment.

### 🤖 AI-Powered Configuration

The system uses **OpenAI** to translate natural language descriptions into precise topic rules (keywords, subreddits), making topic creation fast and intuitive.

## 🏗 Architecture

[![](https://mermaid.ink/img/pako:eNqdVWtP2zAU_SuRJ6QihdKmT8KExKBIbIUVgjRphA8mcdusiZ3ZzqBr-993bSdt0xQklg-JfX3OffgeOwsUsJAgF004TqfW8N6nFjwiezaGwasknOL40UfF0PJYxgMifPRkwOq5J2EYyVrNR2ZknY-ufXR4aBCEhjt-r-mECBkxCo7XY2uI54SX_HqE_4kCYiC1bWy-8vmZH5_VbqGI-i9xCCHfinhDhMCTiE4g4jc8nuFSnBssgykJR0xI8cDSKFgsfMR1KXWp5vVEI6Dq1WpDG1AeKZ6hv8EkOahM_YKD2TiK4wcsZlWiMtafc8iGWC1rxBn0Qpi6NpPyJqaYz2Dz9NfyJCc4AYzZutFcThnd7Jx6hmwiIIXpI5CKMQCe3krCk4zjCYEM8tGeTg485W0QY2hfIAjmwZZLneXdMJJEgczo3YCq-bpk0NmeYGCtmbWPymTEiSBU4lybV5xRqVBlseNAaqnD1zpP06q_gwPrEktsXcXsxVjyc3F0dLb86n2_tbTUlmWBG2TJpAm5OgtORayGZ96VxX0etBLyaFoUClOI2doB7xP5dsR96-86XEuKbovt_Rw8nxp49dxoYmG2lH3vtq4bcwEt5Swu9UY1Urm5H3gPMP2dAW2pLzGa60kv32WEz00222bgh8c_OIh2mcu4vL6TXLUEAw_gcIhLMraEyd5SIPdTo2ELSHhGtoZHL1Eop66Tvp7ucuEE_h-TFBf8R8nl3ba1ouyirbbahLygEmng2WavbH0dm8RLCHNkbNOdIrtTZMPPKgqRK3lGbJQQnmA1RQvF9ZGckgQuDxeGoVI58ukKOCmmPxlLChpn2WSK3DGOBcyyNMSSXEYYLoFkbeVwnAm_YBmVyO01TzraC3IX6BW5Tv2k22u1Gs1Ws9NtN5vtno3myD1p1rv9Tt9p9NuNVttxOisb_dVxm3Wn12_1HaftdBrddrvbtRGB-hi_Mb9g_Sde_QOM0neP?type=png)](https://mermaid.live/edit#pako:eNqdVWtP2zAU_SuRJ6QihdKmT8KExKBIbIUVgjRphA8mcdusiZ3ZzqBr-993bSdt0xQklg-JfX3OffgeOwsUsJAgF004TqfW8N6nFjwiezaGwasknOL40UfF0PJYxgMifPRkwOq5J2EYyVrNR2ZknY-ufXR4aBCEhjt-r-mECBkxCo7XY2uI54SX_HqE_4kCYiC1bWy-8vmZH5_VbqGI-i9xCCHfinhDhMCTiE4g4jc8nuFSnBssgykJR0xI8cDSKFgsfMR1KXWp5vVEI6Dq1WpDG1AeKZ6hv8EkOahM_YKD2TiK4wcsZlWiMtafc8iGWC1rxBn0Qpi6NpPyJqaYz2Dz9NfyJCc4AYzZutFcThnd7Jx6hmwiIIXpI5CKMQCe3krCk4zjCYEM8tGeTg485W0QY2hfIAjmwZZLneXdMJJEgczo3YCq-bpk0NmeYGCtmbWPymTEiSBU4lybV5xRqVBlseNAaqnD1zpP06q_gwPrEktsXcXsxVjyc3F0dLb86n2_tbTUlmWBG2TJpAm5OgtORayGZ96VxX0etBLyaFoUClOI2doB7xP5dsR96-86XEuKbovt_Rw8nxp49dxoYmG2lH3vtq4bcwEt5Swu9UY1Urm5H3gPMP2dAW2pLzGa60kv32WEz00222bgh8c_OIh2mcu4vL6TXLUEAw_gcIhLMraEyd5SIPdTo2ELSHhGtoZHL1Eop66Tvp7ucuEE_h-TFBf8R8nl3ba1ouyirbbahLygEmng2WavbH0dm8RLCHNkbNOdIrtTZMPPKgqRK3lGbJQQnmA1RQvF9ZGckgQuDxeGoVI58ukKOCmmPxlLChpn2WSK3DGOBcyyNMSSXEYYLoFkbeVwnAm_YBmVyO01TzraC3IX6BW5Tv2k22u1Gs1Ws9NtN5vtno3myD1p1rv9Tt9p9NuNVttxOisb_dVxm3Wn12_1HaftdBrddrvbtRGB-hi_Mb9g_Sde_QOM0neP)

## 🚀 Services Overview

### 1. **Ingestion Service** (`node-ts`)

- **Role**: The primary data entry point. Handles both real-time polling and historical backfilling.
- **Features**:
  - **Topic-Specific Search**: Queries Reddit API for posts matching defined keywords/subreddits.
  - **Smart Backfill**: Fetches historical data with progress tracking.
  - **Rate Limit Handling**: Respects Reddit's API limits and optimizes request pacing.
  - **Deduplication**: Ensures same post isn't processed twice.
- **Key Config**: `REDDIT_POLL_INTERVAL_SECONDS`, `REDDIT_LIMIT`.

### 2. **Spark Streaming** (`pyspark`)

- **Role**: The analytics engine. Processes the stream of matched posts to calculate aggregated metrics.
- **Features**:
  - **Structured Streaming**: Fault-tolerant stream processing.
  - **Trend Analysis**: Calculates growth rates and volume velocity.
  - **Windowed Aggregation**: Computes metrics over sliding time windows.
  - **Output**: Writes enriched metrics back to Kafka for indexing.

### 3. **API Service** (`express-ts`)

- **Role**: The control plane and gateway for the frontend.
- **Features**:
  - **Topic Management**: CRUD operations backed by SQLite.
  - **Compute-on-Read**: Aggregates granular data from Elasticsearch for dashboard visualization.
  - **AI Integration**: OpenAI-powered topic generation.
- **Endpoints**:
  - `POST /topics`: Create a new topic.
  - `POST /topics/:id/backfill`: Trigger historical data backfill.
  - `GET /topics/:id/report`: Get full metrics report.

### 4. **Storage & Infrastructure**

- **Kafka**: Central message bus buffering raw and processed events.
- **Elasticsearch**: High-speed search and aggregation engine for processed data.
- **Logstash**: Reliable pipeline to sync Kafka topics to Elasticsearch.
- **SQLite**: Lightweight storage for configuration and topic definitions.

## 🛠 Prerequisites

- **Docker** and **Docker Compose** (Required)
- **Node.js 18+** (for local development)
- **Python 3.9+** (for local development)

## 🚦 Getting Started

1.  **Clone the repository**

    ```bash
    git clone https://github.com/your-repo/trend-insight.git
    cd trend-insight/back-end
    ```

2.  **Configure Environment**
    Copy the template and fill in your details (especially `OPENAI_API_KEY`).

    ```bash
    cp .env.template .env
    ```

3.  **Start Services**

    ```bash
    docker-compose up -d --build
    ```

    _Note: Kafka will take at least 15 seconds to be ready, so do not use the app until then._

4.  **Verify Running Services**
    ```bash
    docker-compose ps
    ```

## 🔌 Access Points

| Service          | URL                     | Description                  |
| :--------------- | :---------------------- | :--------------------------- |
| **API**          | `http://localhost:8000` | Main REST API                |
| **Kafka UI**     | `http://localhost:8085` | Inspect topics and messages  |
| **Kibana**       | `http://localhost:5601` | Visualize Elasticsearch data |
| **Spark Master** | `http://localhost:8080` | Monitor Spark jobs           |
| **Spark Worker** | `http://localhost:8081` | Monitor Spark worker         |
