
import argparse
import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CSV = os.path.join(BASE_DIR, "data", "jobs_clean.csv")


def norm(x: Any) -> str:
    if x is None:
        return "Unknown"
    s = str(x).strip()
    return s if s else "Unknown"


def split_skills(x: Any) -> List[str]:
    if not isinstance(x, str):
        return []
    parts = [p.strip() for p in x.split(",")]
    return [p for p in parts if p]


def create_topic_if_needed(bootstrap: str, topic: str, partitions: int, rf: int) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="jobs-producer-admin")
    try:
        admin.create_topics(
            new_topics=[NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)],
            validate_only=False,
        )
        print(f"[OK] Created topic={topic} partitions={partitions} rf={rf}")
    except TopicAlreadyExistsError:
        print(f"[OK] Topic exists: {topic}")
    except Exception as e:
        print("[WARN] create topic failed (skip):", e)
    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "jobs_raw"))

    parser.add_argument("--create-topic", action="store_true")
    parser.add_argument("--partitions", type=int, default=6)   # tận dụng multi-consumer
    parser.add_argument("--replication", type=int, default=1)

    parser.add_argument("--chunksize", type=int, default=20000)
    parser.add_argument("--max-records", type=int, default=50000, help="0 = send all")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--log-every", type=int, default=20000)

    parser.add_argument("--key-by", choices=["country", "company", "none"], default="country")
    parser.add_argument("--compression", choices=["none", "gzip"], default="none")  # local -> none fastest
    args = parser.parse_args()

    print("[INFO] csv      =", args.csv)
    print("[INFO] bootstrap=", args.bootstrap)
    print("[INFO] topic    =", args.topic)

    if args.create_topic:
        create_topic_if_needed(args.bootstrap, args.topic, args.partitions, args.replication)

    compression = None if args.compression == "none" else args.compression

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
        linger_ms=20,
        batch_size=128 * 1024,
        compression_type=compression,
        acks=1,
        retries=5,
        request_timeout_ms=30000,
    )

    sent = 0
    t0 = time.time()

    # đọc theo chunksize để không ăn RAM
    reader = pd.read_csv(
        args.csv,
        chunksize=args.chunksize,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )

    for chunk in reader:
        # đảm bảo đúng header trong jobs_clean.csv:
        # job_link,job_title,company,search_country,search_city,job_level,job_type,job_skills
        for _, row in chunk.iterrows():
            msg: Dict[str, Any] = {
                "schema_version": 1,
                "job_link": row.get("job_link"),
                "job_title": row.get("job_title"),
                "company": row.get("company"),
                "country": norm(row.get("search_country")),
                "city": norm(row.get("search_city")),
                "job_level": norm(row.get("job_level")),
                "job_type": norm(row.get("job_type")),
                "skills": split_skills(row.get("job_skills", "")),
                "event_ts": int(time.time()),
            }

            if args.key_by == "country":
                key = msg["country"]
            elif args.key_by == "company":
                key = norm(msg.get("company"))
            else:
                key = None

            producer.send(args.topic, value=msg, key=key)
            sent += 1

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000)

            if args.log_every and sent % args.log_every == 0:
                dt = max(time.time() - t0, 1e-6)
                print(f"[PRODUCER] sent={sent:,} | {sent/dt:,.0f} msg/s")

            if args.max_records and sent >= args.max_records:
                break

        if args.max_records and sent >= args.max_records:
            break

    producer.flush()
    producer.close()

    dt = max(time.time() - t0, 1e-6)
    print(f"[DONE] total sent={sent:,} | avg={sent/dt:,.0f} msg/s")


if __name__ == "__main__":
    main()
