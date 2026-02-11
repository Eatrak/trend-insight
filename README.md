# Trend Insight

Trend Insight is a real-time trend tracking system, where the user can define topics, and each topic has a matching pattern in CNF (Conjunctive Normal Form) with keywords.
The system takes the matched posts from Reddit, applies sentiment analysis by using Spark, and shows the data on a dashboard.

![Trend Insight App](./images/app.png)

## Architecture

[![](https://mermaid.ink/img/pako:eNqVVm1v2jAQ_iuWpUkg8dICAYoQUtdSqSvtaFOp0pp-MImBDLAzx2nLgP--s51AQoBtfADf3XPnu8s9F1bY5R7FHTwRJJiiwZPDEHzCaGQU_U9JBSPzVwcnR2TzSLg0dPCbAavPE_U8XxYKDjYndDm8dXCxaBCUeXtxb9mEhtLnzKbi3XcpxN-qUKxDhQfIrfIzLGaueuaB734l7mzsz-c-m1xxFkYLKuDu7qinrShl7lZHve5I9ApdvzeK1WWpUN2qry1uHABNBI8CuKy4u-wyktxeMhcCQXwloVjcwvLF3dMwJBPAQFV3ZDwjmfzviXSn1BvyUIY629XKwTqhysKYKoGyOXiz2Xn1mfCVzXjvO9LYeipC0pNnEs72_Ue7flWksu9ct-UN-CQE2_TVNDoR0_0dc_FBhFdO0inH6ZR1Osf7_bbXQDsgYgbN07-Z5mnNtfDf9fPWEjIiKgyXcspZ9vlpxNU8CmXKIZbzwBcuZskgGajRhEmRUGPBFPevpe018c4fEUbgBnPYAnalSy7IRPEhPqEBWapcU03o2-oZ9OcE6OKGlAh3umuiHrGl_ThQGH3Y2fKjqqmmBxUIe-Aq0BaMLSYlVFntpYh5lARDQUPKJFGMhvA3gjOpUNmtQVypdwb8ossgyMf78gVdE0nQzZx_GE28YMrl3vqb_f0BaSKtj66Fk04pehvcsSjaM2Zu4pwjsgmRivkfXuY7ZzwUITWqxiul0PhkV6A9x0M7JBUh5pGKoHfEOsOeFDDW5JDpnMz3oRtPpphslezKOV2VHd9lK1jMMIcZZX7pmaz33xJIAf42RNuJBLUUfJ4ZSjXBKvRT334G8VcEL7O1fg2ymEja_BhRsdwlnajB36u-CF_StWFv1nws33x5uASvct_DHSkiWsKQ-4IoEa9URNj3U7qgDu7A0dPL1WEb8AkI-8H5InGD7TWZ4s6YzEOQosAjkl77BJi92GoFcJSKKx4xiTuWdaGD4M4Kf-JOuVVrVqz6Ra1-3mw3zmpnrRJe4s651ag06812y6o3mudWu1XblPBvfW-tUrMaTQv82latcQ54CoTl4t78PdH_UjZ_AF_g3DU?type=png)](https://mermaid.live/edit#pako:eNqVVm1v2jAQ_iuWpUkg8dICAYoQUtdSqSvtaFOp0pp-MImBDLAzx2nLgP--s51AQoBtfADf3XPnu8s9F1bY5R7FHTwRJJiiwZPDEHzCaGQU_U9JBSPzVwcnR2TzSLg0dPCbAavPE_U8XxYKDjYndDm8dXCxaBCUeXtxb9mEhtLnzKbi3XcpxN-qUKxDhQfIrfIzLGaueuaB734l7mzsz-c-m1xxFkYLKuDu7qinrShl7lZHve5I9ApdvzeK1WWpUN2qry1uHABNBI8CuKy4u-wyktxeMhcCQXwloVjcwvLF3dMwJBPAQFV3ZDwjmfzviXSn1BvyUIY629XKwTqhysKYKoGyOXiz2Xn1mfCVzXjvO9LYeipC0pNnEs72_Ue7flWksu9ct-UN-CQE2_TVNDoR0_0dc_FBhFdO0inH6ZR1Osf7_bbXQDsgYgbN07-Z5mnNtfDf9fPWEjIiKgyXcspZ9vlpxNU8CmXKIZbzwBcuZskgGajRhEmRUGPBFPevpe018c4fEUbgBnPYAnalSy7IRPEhPqEBWapcU03o2-oZ9OcE6OKGlAh3umuiHrGl_ThQGH3Y2fKjqqmmBxUIe-Aq0BaMLSYlVFntpYh5lARDQUPKJFGMhvA3gjOpUNmtQVypdwb8ossgyMf78gVdE0nQzZx_GE28YMrl3vqb_f0BaSKtj66Fk04pehvcsSjaM2Zu4pwjsgmRivkfXuY7ZzwUITWqxiul0PhkV6A9x0M7JBUh5pGKoHfEOsOeFDDW5JDpnMz3oRtPpphslezKOV2VHd9lK1jMMIcZZX7pmaz33xJIAf42RNuJBLUUfJ4ZSjXBKvRT334G8VcEL7O1fg2ymEja_BhRsdwlnajB36u-CF_StWFv1nws33x5uASvct_DHSkiWsKQ-4IoEa9URNj3U7qgDu7A0dPL1WEb8AkI-8H5InGD7TWZ4s6YzEOQosAjkl77BJi92GoFcJSKKx4xiTuWdaGD4M4Kf-JOuVVrVqz6Ra1-3mw3zmpnrRJe4s651ag06812y6o3mudWu1XblPBvfW-tUrMaTQv82latcQ54CoTl4t78PdH_UjZ_AF_g3DU)

### Ingestion service (`Node.js`)

There two core components in the ingestion service:

- **Syncing Worker**: An infinite loop that fetches all the topics from the system API every 1 minute. For each topic, it queries the matching Reddit posts of the last 48 hours, in order to keep the number of upvotes and comments of the matched posts up to date.
- **Topic Backfilling Worker**: A Kafka consumer that listens for topic-backfilling tasks. For each topic-backfilling task, it queries the matched-posts of the last 30 days of the given topic.

### Kafka

There are 3 Kafka topics:

- `topic.matched.posts` (8 partitions): Matched posts of topics.
- `topic.enriched.matched.posts` (1 partitions): Matched posts of topics with sentiment attribute.
- `topic.backfilling.tasks` (1 partitions): Backfilling tasks for topics.

I dediced 8 partitions for the `topic.matched.posts` topic because in this way 8 Spark tasks are created for each micro-batch, and I can process all of them in parallel and use the full potential of my Spark worker.

**Note:** since the number of cores of the Spark worker is the same of the number of cores of your machine, if your machine have a different number of cores you can change the number of partitions in the `docker-compose.yml` file as you wish.

There are 3 consumer groups:

- `backfill-topic`: consumers (Ingestion Service) that performs topic backfilling.
- `enrich-matched-posts`: consumers (Spark executors) that enriches matched posts with sentiment attribute.
- `forward-enriched-matched-posts`: consumers (Logstash) that forwards enriched matched posts to Elasticsearch.

### Spark Sentiment Analyzer (`Python`)

After the ingestion of the matched posts of topics, Spark runs a sentiment analysis on them, and writes the enriched records into Kafka.

### Logstash

It reads the enriched records from Kafka, add the field `@timestamp` based on the `timestamp` field, and forwards them to Elasticsearch.

### API & Dashboard

The Express.js API uses MySQL to store the topics and it uses Elasticsearch to calculate metrics per time window (1 day, 7 days, 30 days). The metrics are then served to the front-end.

### Kibana

It's used to visualize metrics from Elastisearch data.

## Tech Stack

- **Front-end**: React, Shadcn UI.
- **Back-end**: Node.js, Express.js, MySQL.
- **DevOps**: Kafka, Spark, Elasticsearch, Logstash, Docker.

## Getting Started

1.  **Clone the repo**

    ```bash
    git clone https://github.com/Eatrak/trend-insight.git
    cd trend-insight
    ```

2.  **Start Services (Docker)**

    ```bash
    cd back-end
    cp .env.template .env
    # Add your OPENAI_API_KEY for the AI features
    docker-compose up -d --build
    ```

    _Note: It takes about 30s for Kafka and Elastic to become healthy._

3.  **Configure Kibana (Optional)**

    To enable the Kibana dashboard:
    1.  Go to [http://localhost:5601/app/management/kibana/objects](http://localhost:5601/app/management/kibana/objects)
    2.  Click **Import**
    3.  Select `back-end/kibana/dashboard.ndjson`
    4.  Click **Import**

4.  **Start Frontend**

    ```bash
    cd ../front-end
    npm install
    npm run dev
    ```

5.  **Access**
    - **Frontend**: http://localhost:5173
    - **API**: http://localhost:8000
    - **Kibana**: http://localhost:5601
    - **Kafka UI**: http://localhost:8085
    - **Spark UI**: http://localhost:8080
