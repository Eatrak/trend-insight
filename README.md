# Trend Insight

Trend Insight is a real-time trend tracking system, where the user can define topics, and each topic has a matching pattern in CNF (Conjunctive Normal Form) for the keywords.
The system takes the matched posts from Reddit, applies sentiment analysis by using Spark, and shows the data on a dashboard.

![Trend Insight App](./images/app.png)

## Architecture

[![](https://mermaid.ink/img/pako:eNqVVltvmzAU_iuWpUmplFtJLyGKInVtKnW9LC2VKq30wQGXsBCbGdM2S_Lfd2xDAiHpNh7A537hfAcW2OM-xT0cCBJP0M2DyxBcSTo2jOGHpIKR6NnF-RE5PBUeTVz8YpTV9UB9P5S1movNCZ2Nrlx8cGA0KPO3_F6xgCYy5Myh4i30KPhfs1DGQ7U7yK35MzkohXrkceh9Jd70NYyikAXnnCXpjAqI3R8PtBQVxP3WeNAfi0GtHw7GGbshlVa_FWqJlzlAgeBpDMEONsHOUsmdOfPAEfhXFMrItVq1uFuaJCQAHajqmrxOSSn_WyK9CfVHPJGJznaxcLFOqDkzomasZC5erTZWQyZCJTPW24Y0k37mIe_JI0mm2_bjTb-aUsk3puvybniQgGzybBqdk8X-vnLxToTfyNNpZOk0dDr7-_2y1UAnJmIKzdPPUvM050KEb_p9awoZEtVGcznhrPz-tMZ5lCayYJDRVcUnLqb5IBlVw8lrhBJrprZ_rWyrh9fhmDACAcxhrbCpXHJBAgWH7IRuyFylWujB0FGvYBgRQIuXUCK8yaaHesLmzv2N0tGHjaw6qRppek4BrztCAbdmZBkmocrWoIDLvRgYCZpQJokCNLi_FJxJpVVeGsSTemXAE53FcdXfly_ogkiCLiP-bjjZfmk0Bstvzvc7pHG03LsVPjUqoNvo7fOiLTPg5sYVHBsXBZ__YWXuFeEuD4VJNVYFhtbPVwXaMty1QgoeMhgpD3pFLEvgKShmnIpmMSdz3xXx0xTzpVLeOJ9X5WSxHKWWIcxlhlndeSbr7Y8EUgp_G6L1RAJbCh6VhlJNsHL9MHQegfyVwrdsqb-CLAOSFt-nVMw3SedssPdbTyKUdGnQWxbvy7daHq7Dlzz0cU-KlNYx5D4jisQL5RHW_YTOqIt7cPT1bnXZCmxiwn5wPsvNYHsFE9x7JVECVBr7RNKLkACyZ2uuAIxScc5TJnGve3qsneDeAn_gXuPQtk6bJx3bPrGsjg3SkzqeK_5x02pbR53OabttdW27a6_q-LeObDVPO8fd7mG3fXx0aNuWVccUMMvFrflB0f8pqz9i39xs?type=png)](https://mermaid.live/edit#pako:eNqVVltvmzAU_iuWpUmplFtJLyGKInVtKnW9LC2VKq30wQGXsBCbGdM2S_Lfd2xDAiHpNh7A537hfAcW2OM-xT0cCBJP0M2DyxBcSTo2jOGHpIKR6NnF-RE5PBUeTVz8YpTV9UB9P5S1movNCZ2Nrlx8cGA0KPO3_F6xgCYy5Myh4i30KPhfs1DGQ7U7yK35MzkohXrkceh9Jd70NYyikAXnnCXpjAqI3R8PtBQVxP3WeNAfi0GtHw7GGbshlVa_FWqJlzlAgeBpDMEONsHOUsmdOfPAEfhXFMrItVq1uFuaJCQAHajqmrxOSSn_WyK9CfVHPJGJznaxcLFOqDkzomasZC5erTZWQyZCJTPW24Y0k37mIe_JI0mm2_bjTb-aUsk3puvybniQgGzybBqdk8X-vnLxToTfyNNpZOk0dDr7-_2y1UAnJmIKzdPPUvM050KEb_p9awoZEtVGcznhrPz-tMZ5lCayYJDRVcUnLqb5IBlVw8lrhBJrprZ_rWyrh9fhmDACAcxhrbCpXHJBAgWH7IRuyFylWujB0FGvYBgRQIuXUCK8yaaHesLmzv2N0tGHjaw6qRppek4BrztCAbdmZBkmocrWoIDLvRgYCZpQJokCNLi_FJxJpVVeGsSTemXAE53FcdXfly_ogkiCLiP-bjjZfmk0Bstvzvc7pHG03LsVPjUqoNvo7fOiLTPg5sYVHBsXBZ__YWXuFeEuD4VJNVYFhtbPVwXaMty1QgoeMhgpD3pFLEvgKShmnIpmMSdz3xXx0xTzpVLeOJ9X5WSxHKWWIcxlhlndeSbr7Y8EUgp_G6L1RAJbCh6VhlJNsHL9MHQegfyVwrdsqb-CLAOSFt-nVMw3SedssPdbTyKUdGnQWxbvy7daHq7Dlzz0cU-KlNYx5D4jisQL5RHW_YTOqIt7cPT1bnXZCmxiwn5wPsvNYHsFE9x7JVECVBr7RNKLkACyZ2uuAIxScc5TJnGve3qsneDeAn_gXuPQtk6bJx3bPrGsjg3SkzqeK_5x02pbR53OabttdW27a6_q-LeObDVPO8fd7mG3fXx0aNuWVccUMMvFrflB0f8pqz9i39xs)

### 1. Ingestion (`Node.js`)

There two core components in the ingestion service:

- **Syncing Worker**: An infinite loop that fetches all the topics from the system API every 1 minute. For each topic, it queries the matching Reddit posts of the last 48 hours, in order to keep the number of upvotes and comments of the matched posts up to date.
- **Topic Backfilling Worker**: A Kafka consumer that listens for topic-backfilling tasks. For each topic-backfilling task, it queries the matched-posts of the last 30 days of the given topic.

### 2. Spark Sentiment Analysis (`Python`)

After the ingestion of the matched posts of topics, Spark runs a sentiment analysis on them, and writes the enriched records into Kafka.

### 3. Logstash

Reads the enriched records from Kafka and forwards them to Elasticsearch.

### 4. API & Dashboard

The Express.js API uses MySQL to store the topics and it uses Elasticsearch to calculate metrics per time window (1 day, 7 days, 30 days). The metrics are then served to the front-end.

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
