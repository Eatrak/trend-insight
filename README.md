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

[![](https://mermaid.ink/img/pako:eNqVVm1v2jAQ_iuWpUog8U4JBSGkrqVSN9rRplKlNf1gEhcygp05TlsG_Ped7QQS3rblA_junjvfHfdcWGKXexR38USQcIqGjw5D8ETx2CgGn5IKRoIXB6dHZPNYuDRy8KsBq-eRep4vCwUHmxO6HN06uFg0CMq8nbi3bEIj6XNmU_HuuxTib1Qo0aHCPeRW-RkVc1c98dB3vxB39uYHgc8mV5xF8ZwKuLs37msryph71XG_Nxb9Qs_vjxN1WSpUr-pri5sEQBPB4xAuK24vu4wltxfMhUAQX0koETew_eLuaBSRCWCgqm_kbUZy-d8R6U6pN-KRjHS2y6WDdUKVuTFVQmVz8Hq99Row4Sub8d51pIn1VIS0J08kmu36j7f9qkhl37puyhvySQS26YtpdCpm-_vGxQcRXjlNp5ykU9bpHO_3604D7ZCIGTRPf-eapzXXwn_Xv7eWkBFRYbSQU87yv59GXAVxJDMOibwPfOZilg6SgRpNWiOUWDC1_Wtlx0bEllyQiRr75ISGZKFSytQ6sFWrBwEBVrgRJcKdbnulc34Y-pIqkDltrQcuVJzSEwnMPHAZaAvGlrAP6qn2Mww8WspI0IgySRR1IfyN4EwqVH49EFfq5QDf6DIM9-OdnaFrIgm6CfiH0SSbpFzur77a3--RZszqKP9POmV4bHDHomjPhKKp8x5jTYhMzP_wMp97xkMRMjNpvDIKjU-XAtpxPLQsMhESwqgIehmscjTJABPNHjKbk_k8dOPJFNP1kd8tp6uyHWbg-6vMpLi7-5EC_G1iNuMHail4kJtANa4q9OPAfgLxVwyvqJV-ubGENdr8EFOxMBlm1eDvVZ8FUHOVkDVvP5bwfn24BG9o38NdKWJawpD8nCgRL1VEWONTOgf-d-Ho6Z3psDX4hIT94HyeusFWmkxx940EEUhx6BFJr30CPJ5vtAIYScUVj5nE3Xq70dFRcHeJP3G3bHVqFavVspptC55Gs4QXgDq3KlbdOr9oNa1ap3leb6xL-Le-t1Fp1Zqt2kWzUbPanXa9XsIU-MnFnfnbof99rP8AKfPROw?type=png)](https://mermaid.live/edit#pako:eNqVVm1v2jAQ_iuWpUog8U4JBSGkrqVSN9rRplKlNf1gEhcygp05TlsG_Ped7QQS3rblA_junjvfHfdcWGKXexR38USQcIqGjw5D8ETx2CgGn5IKRoIXB6dHZPNYuDRy8KsBq-eRep4vCwUHmxO6HN06uFg0CMq8nbi3bEIj6XNmU_HuuxTib1Qo0aHCPeRW-RkVc1c98dB3vxB39uYHgc8mV5xF8ZwKuLs37msryph71XG_Nxb9Qs_vjxN1WSpUr-pri5sEQBPB4xAuK24vu4wltxfMhUAQX0koETew_eLuaBSRCWCgqm_kbUZy-d8R6U6pN-KRjHS2y6WDdUKVuTFVQmVz8Hq99Row4Sub8d51pIn1VIS0J08kmu36j7f9qkhl37puyhvySQS26YtpdCpm-_vGxQcRXjlNp5ykU9bpHO_3604D7ZCIGTRPf-eapzXXwn_Xv7eWkBFRYbSQU87yv59GXAVxJDMOibwPfOZilg6SgRpNWiOUWDC1_Wtlx0bEllyQiRr75ISGZKFSytQ6sFWrBwEBVrgRJcKdbnulc34Y-pIqkDltrQcuVJzSEwnMPHAZaAvGlrAP6qn2Mww8WspI0IgySRR1IfyN4EwqVH49EFfq5QDf6DIM9-OdnaFrIgm6CfiH0SSbpFzur77a3--RZszqKP9POmV4bHDHomjPhKKp8x5jTYhMzP_wMp97xkMRMjNpvDIKjU-XAtpxPLQsMhESwqgIehmscjTJABPNHjKbk_k8dOPJFNP1kd8tp6uyHWbg-6vMpLi7-5EC_G1iNuMHail4kJtANa4q9OPAfgLxVwyvqJV-ubGENdr8EFOxMBlm1eDvVZ8FUHOVkDVvP5bwfn24BG9o38NdKWJawpD8nCgRL1VEWONTOgf-d-Ho6Z3psDX4hIT94HyeusFWmkxx940EEUhx6BFJr30CPJ5vtAIYScUVj5nE3Xq70dFRcHeJP3G3bHVqFavVspptC55Gs4QXgDq3KlbdOr9oNa1ap3leb6xL-Le-t1Fp1Zqt2kWzUbPanXa9XsIU-MnFnfnbof99rP8AKfPROw)

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
