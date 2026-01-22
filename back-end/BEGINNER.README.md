# Beginner's Guide to Reddit Insight

Welcome to the **Reddit Insight** project! 👋

## 🏗️ The Big Picture

Imagine a factory assembly line. Raw materials (Reddit posts) come in one end, get processed, measured, and packaged, and finished reports come out the other end.

Here is what our "Assembly Line" looks like:

[![](https://mermaid.ink/img/pako:eNpNkU1vgkAQhv_KZk5tokZApHBoUsFW0zZpNb0UPExghE1hIbtLo1X_e1ekH3vamXnemXl3D5DWGUEAucSmYE-rRDBz7uIVZRnX7O5luWHD4e3xpS5LdWSzq6XISWleC7Ym-clTur5IZh22JpEpFqHGIwsPj7j9wNOlHnb1FaGpr7UkrI4sitcNyo8-5iLfXNioY0Ms07ZETYo9k5Y8NfPD_8367G-7xdVTnSuNquh3WnTYUmS0IyOex_MSze6pIpRp0Q-bd9BrS5KfofvYeP7x1iP3HRJx1ZS4N8xD_KZIbmBgno1nEGjZ0gAqkhWeQzicZQnogipKIDDXzNhMIBEno2lQvNd19SOTdZsXEGyxVCZqm8w4jjiaD_lDzKuSDOtWaAi8SdcCggPsILAsf-T4lmt7lj31fc_yB7A3ac8Z-RPXssbuxHXsqeOdBvDVTR2PvKk7tm3burFtz7mZuqdvDJSh1w?type=png)](https://mermaid.live/edit#pako:eNpNkU1vgkAQhv_KZk5tokZApHBoUsFW0zZpNb0UPExghE1hIbtLo1X_e1ekH3vamXnemXl3D5DWGUEAucSmYE-rRDBz7uIVZRnX7O5luWHD4e3xpS5LdWSzq6XISWleC7Ym-clTur5IZh22JpEpFqHGIwsPj7j9wNOlHnb1FaGpr7UkrI4sitcNyo8-5iLfXNioY0Ms07ZETYo9k5Y8NfPD_8367G-7xdVTnSuNquh3WnTYUmS0IyOex_MSze6pIpRp0Q-bd9BrS5KfofvYeP7x1iP3HRJx1ZS4N8xD_KZIbmBgno1nEGjZ0gAqkhWeQzicZQnogipKIDDXzNhMIBEno2lQvNd19SOTdZsXEGyxVCZqm8w4jjiaD_lDzKuSDOtWaAi8SdcCggPsILAsf-T4lmt7lj31fc_yB7A3ac8Z-RPXssbuxHXsqeOdBvDVTR2PvKk7tm3burFtz7mZuqdvDJSh1w)

---

## 🧑‍🤝‍🧑 Meet the Team (The Containers)

This project runs on **Docker**, which means it's a collection of small "mini-computers" (containers) talking to each other. Here is who they are:

### 1. `ingestion` (The Scout) 🕵️
*   **Job**: It goes to Reddit every few seconds and asks "What's new?".
*   **Action**: It grabs new posts and comments and throws them into **Kafka**.
*   **Analogy**: Like a news reporter calling in stories to the newsroom.

### 2. `kafka` (The Mailbox) 📬
*   **Job**: It holds messages until someone is ready to read them.
*   **Action**: It has "topics" (folders) like `reddit.raw.posts`.
*   **Analogy**: A giant inbox where letters wait. It ensures no data is lost even if the processor is slow.

### 3. `spark-streaming` (The Brain) 🧠
*   **Job**: The heavy lifter. It reads from Kafka, filters data, and does the math.
*   **Action**: 
    *   It looks for keywords (e.g., "Apple", "Election").
    *   It calculates **Velocity** (how fast are people talking?) and **Acceleration** (is it going viral?).
*   **Analogy**: The analyst who reads 1,000 letters a minute and creates a summary report.

### 4. `logstash` (The Courier) 🚚
*   **Job**: It moves data from one place to another.
*   **Action**: It continually reads from the **Kafka** "Metrics" folder and delivers it safely to the **Elasticsearch** database.
*   **Analogy**: The delivery truck that takes the finished reports from the mailbox and puts them in the library's filing cabinet.

### 5. `api` (The Librarian) 📚
*   **Job**: It answers your questions.
*   **Action**: When you ask "How is 'Bitcoin' doing?", it looks up the latest numbers calculated by Spark and gives you a JSON report.

---

## 🌊 How Data Flows (A Story)

1.  User `u/cool_guy` posts about **"SpaceX"** on `r/technology`.
2.  **Ingestion** sees this post 10 seconds later and sends it to **Kafka**.
3.  **Spark** reads the post from **Kafka**. It checks its list: "Are we tracking 'SpaceX'? Yes!"
4.  **Spark** updates the score for 'SpaceX':
    *   *Mentions: +1*
    *   *Engagement: +50 (upvotes)*
5.  **Spark** saves this new score to the database.
6.  You run a `curl` command to the **API**, and you see the numbers go up! 📈

---

## 🚀 Getting Started

### Prerequisites
You only need **Docker Desktop** installed. That's it!

### 1. Start the System
Open your terminal in this folder and run:
```bash
docker compose up --build
```
*Wait about 2-3 minutes.* You will see lots of colorful logs. Wait until things settle down.
*Note: The first time you run this, it will fetch the last 24 hours of data from Reddit automatically!*

### 2. Check if it's working
Open a new terminal and ask the API for a report on "World Events" (a default topic):
```bash
curl http://localhost:8000/topics/world-events/report
```
If you see a JSON response with numbers, **IT WORKS!** 🎉

### 3. Stop the System
To shut everything down cleanly:
```bash
docker compose down
```

---

## 🛠️ Troubleshooting for Beginners

**"I see no data!"**
*   Did you wait a few minutes? Spark takes a moment to warm up.
*   Check the logs: `docker compose logs spark-streaming`
*   Are you getting errors? Reset the whole thing:
    ```bash
    docker compose down -v  # The -v deletes old data so you start fresh
    docker compose up --build
    ```

**"The logs are moving too fast!"**
*   That's normal! But if you want to see just one service, use:
    ```bash
    docker compose logs -f ingestion
    ```

---

Happy Coding! 💻
