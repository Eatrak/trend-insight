import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from kafka import KafkaProducer




def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: module loaded. Kafka={KAFKA_BOOTSTRAP_SERVERS}", flush=True)
USER_AGENT = _env("REDDIT_USER_AGENT", "trend-insight/0.1")
POLL_INTERVAL_SECONDS = int(_env("REDDIT_POLL_INTERVAL_SECONDS", "15"))
SUBREDDITS = _env("REDDIT_SUBREDDITS", "all")
LIMIT = int(_env("REDDIT_LIMIT", "50"))


def utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        linger_ms=50,
        acks="all",
        retries=5,
    )


def reddit_get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, params=params, timeout=20)
    # 429 happens easily if polling too aggressively
    if r.status_code == 429:
        retry_after = int(r.headers.get("retry-after") or "5")
        time.sleep(retry_after)
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, params=params, timeout=20)
    r.raise_for_status()
    return r.json()



def get_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_post(child: dict[str, Any]) -> Optional[dict[str, Any]]:
    d = child.get("data") or {}
    if not d.get("id"):
        return None
    
    # Strict Schema from README
    return {
        "event_id": d.get("name") or d.get("id"),
        "event_type": "post",
        "subreddit": d.get("subreddit") or "",
        "author": d.get("author") or None,
        "created_utc": utc_iso(float(d.get("created_utc") or time.time())),
        "text": (d.get("title") or "") + "\n" + (d.get("selftext") or ""),
        "score": int(d.get("score") or 0),
        "num_comments": int(d.get("num_comments") or 0),
        "ingested_at": get_now_iso(),
        # Internal fields for debugging only, not strictly required by schema but useful
        # "permalink": d.get("permalink") or "",
        # "url": d.get("url") or "",
    }


def extract_comment(child: dict[str, Any]) -> Optional[dict[str, Any]]:
    d = child.get("data") or {}
    if not d.get("id"):
        return None
        
    return {
        "event_id": d.get("name") or d.get("id"),
        "event_type": "comment",
        "subreddit": d.get("subreddit") or "",
        "author": d.get("author") or None,
        "created_utc": utc_iso(float(d.get("created_utc") or time.time())),
        "text": d.get("body") or "",
        "score": int(d.get("score") or 0),
        "num_comments": 0, # Logic: Comments are leaves in this polling model
        "ingested_at": get_now_iso(),
    }


def poll_loop() -> None:
    print("[ingestion] Initializing producer...", flush=True)
    producer = None
    while producer is None:
        try:
            producer = make_producer()
            print("[ingestion] Producer connected!", flush=True)
        except Exception as e:
            print(f"[ingestion] Failed to connect to Kafka ({e}). Retrying in 5s...", flush=True)
            time.sleep(5)

    last_seen_posts: set[str] = set()
    last_seen_comments: set[str] = set()

    # Sanitized Subreddits
    safe_subreddits = SUBREDDITS.replace(",", "+")
    posts_url = f"https://www.reddit.com/r/{safe_subreddits}/new.json"
    comments_url = f"https://www.reddit.com/r/{safe_subreddits}/comments.json"

    # --- Backfill Logic ---
    # --- Backfill Logic ---
    STATE_FILE = "/app/backfill_state.json"
    if os.path.exists(STATE_FILE):
        print(f"[ingestion] Found state file at {STATE_FILE}. Skipping backfill.", flush=True)
    else:
        print("[ingestion] Starting 7 days backfill...", flush=True)
        backfill_cutoff = time.time() - (7 * 24 * 3600) # 7 days ago
        after: Optional[str] = None
        fetched_count = 0
        keep_fetching = True
        
        while keep_fetching:
            try:
                params = {"limit": 100} # Max allowed by Reddit
                if after:
                    params["after"] = after
                
                print(f"[ingestion] Backfill fetch offset={fetched_count} after={after}")
                data = reddit_get_json(posts_url, params)
                children = (data.get("data") or {}).get("children") or []
                
                if not children:
                    print("[ingestion] Backfill done (no more data).")
                    break

                for child in children:
                    msg = extract_post(child)
                    if not msg: continue
                    
                    # Time check
                    created_ts = float(child["data"].get("created_utc") or 0)
                    if created_ts < backfill_cutoff:
                        print(f"[ingestion] Reached cutoff time (7 days ago). Stopping backfill.")
                        keep_fetching = False
                        break # Stop processing this batch
                    
                    # dedupe
                    if msg["event_id"] in last_seen_posts: continue
                    
                    producer.send("reddit.raw.posts", key=msg["event_id"], value=msg)
                    last_seen_posts.add(msg["event_id"])
                    
                    # Update after token
                    after = child["data"]["name"] # 't3_xxxxx'
                
                fetched_count += len(children)
                if fetched_count > 100000: # Safety cap to prevent infinite loops on very active subs
                     print("[ingestion] Hit safety cap (100000 posts). Stopping backfill.")
                     break

                producer.flush()
                time.sleep(1) # be nice during backfill

            except Exception as e:
                print(f"[ingestion] Backfill error: {e}")
                break
                
        print(f"[ingestion] Backfill complete. Fetched {fetched_count} posts. Switching to poll loop.")
        # Write state
        try:
            with open(STATE_FILE, "w") as f:
                json.dump({"completed_at": get_now_iso()}, f)
            print(f"[ingestion] Wrote backfill state to {STATE_FILE}", flush=True)
        except Exception as e:
            print(f"[ingestion] Failed to write state file: {e}", flush=True)

    # --- Main Poll Loop ---
    while True:
        try:
            # Poll Posts
            posts = reddit_get_json(posts_url, {"limit": LIMIT})
            for child in (posts.get("data") or {}).get("children") or []:
                msg = extract_post(child)
                if not msg: continue
                if msg["event_id"] in last_seen_posts: continue
                
                producer.send("reddit.raw.posts", key=msg["event_id"], value=msg)
                last_seen_posts.add(msg["event_id"])
            
            # Simple cap
            if len(last_seen_posts) > 10000:
                last_seen_posts = set(list(last_seen_posts)[-5000:])

            # Poll Comments (Backfill not strictly needed for comments as velocity relies mainly on post counts for now)
            comments = reddit_get_json(comments_url, {"limit": LIMIT})
            for child in (comments.get("data") or {}).get("children") or []:
                msg = extract_comment(child)
                if not msg: continue
                if msg["event_id"] in last_seen_comments: continue
                
                producer.send("reddit.raw.comments", key=msg["event_id"], value=msg)
                last_seen_comments.add(msg["event_id"])
            
            if len(last_seen_comments) > 10000:
                last_seen_comments = set(list(last_seen_comments)[-5000:])

            producer.flush(timeout=10)
        except Exception as e:  # noqa: BLE001
            print(f"[ingestion] error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    print(f"[ingestion] kafka={KAFKA_BOOTSTRAP_SERVERS} subreddits={SUBREDDITS} interval={POLL_INTERVAL_SECONDS}s")
    poll_loop()

