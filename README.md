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

[![](https://mermaid.ink/img/pako:eNqdVm1v2jAQ_iuWp0ogBQppCyVFSF1LpW5t1zaVJq3ZB5MYyAA7s521jPLfd7aTkvDSreMD2Hf33JvvsVngkEcUe3gkSDJGV_cBQ_CR6cAK-s-KCkamjwHOl8jnqQipDPB3a6w_9zSKYlWpBNiu0OntZYCrVWtBWbTm95KNqFQxZz4Vv-KQgv9XEcpkqHIDudV_yGop1ANP4vAjCSfDeDqN2eiMM5nOqIDY3UHPaFFB3d0f9LoD0at0494gE9eUturux0YTZg7QSPA0gWDVVbDTVHF_zkJwBP71DmXbV7PN4q6plGQENlDVZzKckFL-10SFYxrdcqmkyXaxCLBJqD6zqnqidQFeLleoPhOx1ln0OpBm2rc85D15IHKyjh-s-lVXWr-CvpZ3xUcSdONH2-h8W-zvkIsnIqJank4tS6dm0tnd76w7fkLExHo3S-QrQckMkkKV27kac1bNo0Gwio3yrzHWzshXXJCRnrtsha7InIrSSfV9XWt_SmAsQ0mJCMerZE3Cd1exotrIrlbazaEwQ21GAqixJRhIK1aXjT-Usd8rUGDnuN0KKilTRHMH3F8IzpS2KvOThMqwE37RaZJs-tvbQ-dEEXQx5U9WklG5Vuu9fPK_3CAzsi87CfgmqEAka7fLi0FmHMnBG5SxLgo-34Gy3xvKbR7MFBZm09jkTERrxtsYWoy4Tf-mw5xhZfq9nYMfMGu-yXYD3LgekTb425m-DgiIleDT0ozogdKu7_v-A2x_pnCLv5j7n2VzbdR3KRVzm2FRDPho_6sA8rxkdCrrdyW8WZ_FhcBWeU6HSGaPiDbyPjQajoTMJ7SwrD3FkRp7bvJ8so6FK-H_kDR_JN8LzllvX0HHjJuTn7mju5EVVAL1fcc2zTHvjE28ZGHp6NhjyrM7wQ48-HGEPSVS6mA46BnRW7zQWHgVxnQGt5kHy0hTAAdsCZiEsG-cz3IYXK2jMfaGZCphlyYRUfQ8JnArrUzgeqHijKdMYc9tH7cPjRfsLfAz9mqttls_aDdaraNm5_iofQzaOfaabrveBInbdFuHjeZBp7N08G8T2K23Wu7Bcad9dOQedhruASAoFMjFtf0fY_7OLP8ARX3taw?type=png)](https://mermaid.live/edit#pako:eNqdVm1v2jAQ_iuWp0ogBQppCyVFSF1LpW5t1zaVJq3ZB5MYyAA7s521jPLfd7aTkvDSreMD2Hf33JvvsVngkEcUe3gkSDJGV_cBQ_CR6cAK-s-KCkamjwHOl8jnqQipDPB3a6w_9zSKYlWpBNiu0OntZYCrVWtBWbTm95KNqFQxZz4Vv-KQgv9XEcpkqHIDudV_yGop1ANP4vAjCSfDeDqN2eiMM5nOqIDY3UHPaFFB3d0f9LoD0at0494gE9eUturux0YTZg7QSPA0gWDVVbDTVHF_zkJwBP71DmXbV7PN4q6plGQENlDVZzKckFL-10SFYxrdcqmkyXaxCLBJqD6zqnqidQFeLleoPhOx1ln0OpBm2rc85D15IHKyjh-s-lVXWr-CvpZ3xUcSdONH2-h8W-zvkIsnIqJank4tS6dm0tnd76w7fkLExHo3S-QrQckMkkKV27kac1bNo0Gwio3yrzHWzshXXJCRnrtsha7InIrSSfV9XWt_SmAsQ0mJCMerZE3Cd1exotrIrlbazaEwQ21GAqixJRhIK1aXjT-Usd8rUGDnuN0KKilTRHMH3F8IzpS2KvOThMqwE37RaZJs-tvbQ-dEEXQx5U9WklG5Vuu9fPK_3CAzsi87CfgmqEAka7fLi0FmHMnBG5SxLgo-34Gy3xvKbR7MFBZm09jkTERrxtsYWoy4Tf-mw5xhZfq9nYMfMGu-yXYD3LgekTb425m-DgiIleDT0ozogdKu7_v-A2x_pnCLv5j7n2VzbdR3KRVzm2FRDPho_6sA8rxkdCrrdyW8WZ_FhcBWeU6HSGaPiDbyPjQajoTMJ7SwrD3FkRp7bvJ8so6FK-H_kDR_JN8LzllvX0HHjJuTn7mju5EVVAL1fcc2zTHvjE28ZGHp6NhjyrM7wQ48-HGEPSVS6mA46BnRW7zQWHgVxnQGt5kHy0hTAAdsCZiEsG-cz3IYXK2jMfaGZCphlyYRUfQ8JnArrUzgeqHijKdMYc9tH7cPjRfsLfAz9mqttls_aDdaraNm5_iofQzaOfaabrveBInbdFuHjeZBp7N08G8T2K23Wu7Bcad9dOQedhruASAoFMjFtf0fY_7OLP8ARX3taw)

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
