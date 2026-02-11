# Trend Insight

Trend Insight is a real-time trend tracking system, where the user can define topics, and each topic has a matching pattern in CNF (Conjunctive Normal Form) for the keywords.
The system takes the matched posts from Reddit, applies sentiment analysis by using Spark, and shows the data on a dashboard.

![Trend Insight App](./images/app.png)

## Architecture

[![](https://mermaid.ink/img/pako:eNqVVm1v2jAQ_iuWpUog8U4JBSGkrqVSN9rRplKlNf1gEhcygp05TlsG_Ped7QQS3rblA_junjvfHfdcWGKXexR38USQcIqGjw5D8ETx2CgGn5IKRoIXB6dHZPNYuDRy8KsBq-eRep4vCwUHmxO6HN06uFg0CMq8nbi3bEIj6XNmU_HuuxTib1Qo0aHCPeRW-RkVc1c98dB3vxB39uYHgc8mV5xF8ZwKuLs37msryph71XG_Nxb9Qs_vjxN1WSpUr-pri5sEQBPB4xAuK24vu4wltxfMhUAQX0koETew_eLuaBSRCWCgqm_kbUZy-d8R6U6pN-KRjHS2y6WDdUKVuTFVQmVz8Hq99Row4Sub8d51pIn1VIS0J08kmu36j7f9qkhl37puyhvySQS26YtpdCpm-_vGxQcRXjlNp5ykU9bpHO_3604D7ZCIGTRPf-eapzXXwn_Xv7eWkBFRYbSQU87yv59GXAVxJDMOibwPfOZilg6SgRpNWiOUWDC1_Wtlx0bEllyQiRr75ISGZKFSytQ6sFWrBwEBVrgRJcKdbnulc34Y-pIqkDltrQcuVJzSEwnMPHAZaAvGlrAP6qn2Mww8WspI0IgySRR1IfyN4EwqVH49EFfq5QDf6DIM9-OdnaFrIgm6CfiH0SSbpFzur77a3--RZszqKP9POmV4bHDHomjPhKKp8x5jTYhMzP_wMp97xkMRMjNpvDIKjU-XAtpxPLQsMhESwqgIehmscjTJABPNHjKbk_k8dOPJFNP1kd8tp6uyHWbg-6vMpLi7-5EC_G1iNuMHail4kJtANa4q9OPAfgLxVwyvqJV-ubGENdr8EFOxMBlm1eDvVZ8FUHOVkDVvP5bwfn24BG9o38NdKWJawpD8nCgRL1VEWONTOgf-d-Ho6Z3psDX4hIT94HyeusFWmkxx940EEUhx6BFJr30CPJ5vtAIYScUVj5nE3Xq70dFRcHeJP3G3bHVqFavVspptC55Gs4QXgDq3KlbdOr9oNa1ap3leb6xL-Le-t1Fp1Zqt2kWzUbPanXa9XsIU-MnFnfnbof99rP8AKfPROw?type=png)](https://mermaid.live/edit#pako:eNqVVm1v2jAQ_iuWpUog8U4JBSGkrqVSN9rRplKlNf1gEhcygp05TlsG_Ped7QQS3rblA_junjvfHfdcWGKXexR38USQcIqGjw5D8ETx2CgGn5IKRoIXB6dHZPNYuDRy8KsBq-eRep4vCwUHmxO6HN06uFg0CMq8nbi3bEIj6XNmU_HuuxTib1Qo0aHCPeRW-RkVc1c98dB3vxB39uYHgc8mV5xF8ZwKuLs37msryph71XG_Nxb9Qs_vjxN1WSpUr-pri5sEQBPB4xAuK24vu4wltxfMhUAQX0koETew_eLuaBSRCWCgqm_kbUZy-d8R6U6pN-KRjHS2y6WDdUKVuTFVQmVz8Hq99Row4Sub8d51pIn1VIS0J08kmu36j7f9qkhl37puyhvySQS26YtpdCpm-_vGxQcRXjlNp5ykU9bpHO_3604D7ZCIGTRPf-eapzXXwn_Xv7eWkBFRYbSQU87yv59GXAVxJDMOibwPfOZilg6SgRpNWiOUWDC1_Wtlx0bEllyQiRr75ISGZKFSytQ6sFWrBwEBVrgRJcKdbnulc34Y-pIqkDltrQcuVJzSEwnMPHAZaAvGlrAP6qn2Mww8WspI0IgySRR1IfyN4EwqVH49EFfq5QDf6DIM9-OdnaFrIgm6CfiH0SSbpFzur77a3--RZszqKP9POmV4bHDHomjPhKKp8x5jTYhMzP_wMp97xkMRMjNpvDIKjU-XAtpxPLQsMhESwqgIehmscjTJABPNHjKbk_k8dOPJFNP1kd8tp6uyHWbg-6vMpLi7-5EC_G1iNuMHail4kJtANa4q9OPAfgLxVwyvqJV-ubGENdr8EFOxMBlm1eDvVZ8FUHOVkDVvP5bwfn24BG9o38NdKWJawpD8nCgRL1VEWONTOgf-d-Ho6Z3psDX4hIT94HyeusFWmkxx940EEUhx6BFJr30CPJ5vtAIYScUVj5nE3Xq70dFRcHeJP3G3bHVqFavVspptC55Gs4QXgDq3KlbdOr9oNa1ap3leb6xL-Le-t1Fp1Zqt2kWzUbPanXa9XsIU-MnFnfnbof99rP8AKfPROw)

### How it works

#### 1. Ingestion (`Node.js`)

There two core components in the ingestion service:

- **Syncing Worker**: An infinite loop that fetches all the topics from the system API every 1 minute. For each topic, it queries the matching Reddit posts of the last 48 hours, in order to keep the number of upvotes and comments of the matched posts up to date.
- **Topic Backfilling Worker**: A Kafka consumer that listens for topic-backfilling tasks. For each topic-backfilling task, it queries the matched-posts of the last 30 days of the given topic.

#### 2. Spark Sentiment Analysis (`Python`)

After the ingestion of the matched posts of topics, Spark runs a sentiment analysis on them, and writes the enriched records into Kafka.

#### 3. Logstash

Reads the enriched records from Kafka and forwards them to Elasticsearch.

#### 4. API & Dashboard

The Express.js API uses SQLite to store the topics and it uses Elasticsearch to calculate metrics per time window (1 day, 7 days, 30 days). The metrics are then served to the front-end.

## Tech Stack

- **Front-end**: React, Shadcn UI.
- **Back-end**: Node.js, Express.js, SQLite.
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

3.  **Start Frontend**

    ```bash
    cd ../front-end
    npm install
    npm run dev
    ```

4.  **Access**
    - **Frontend**: http://localhost:5173
    - **API**: http://localhost:8000
    - **Kafka UI**: http://localhost:8085
    - **Spark UI**: http://localhost:8080
