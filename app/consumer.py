from kafka import KafkaConsumer
import argparse
import csv
import json
import os
import time
from collections import Counter, defaultdict
from typing import Any, Dict


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR_DEFAULT = os.path.join(BASE_DIR, "data")


def norm(x: Any) -> str:
    if x is None:
        return "Unknown"
    s = str(x).strip()
    return s if s else "Unknown"


def prune_counter(c: Counter, keep: int) -> None:
    if len(c) <= keep:
        return
    top = c.most_common(keep)
    c.clear()
    c.update(dict(top))


def write_outputs(out_dir: str, top_n: int,
                  skill_by_country: Dict[str, Counter],
                  jobtype_by_country: Dict[str, Counter]) -> None:
    os.makedirs(out_dir, exist_ok=True)

    skills_path = os.path.join(out_dir, "top_skills_by_country.csv")
    with open(skills_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["country", "skill", "count"])
        for country in sorted(skill_by_country.keys()):
            for skill, cnt in skill_by_country[country].most_common(top_n):
                w.writerow([country, skill, cnt])

    jt_path = os.path.join(out_dir, "jobtype_by_country.csv")
    with open(jt_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["country", "job_type", "count"])
        for country in sorted(jobtype_by_country.keys()):
            for jt, cnt in jobtype_by_country[country].most_common():
                w.writerow([country, jt, cnt])

    print("[SAVED]")
    print(" -", skills_path)
    print(" -", jt_path)


def try_plot(out_dir: str) -> None:
    # Optional: needs matplotlib + pandas
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
    except Exception as e:
        print("[PLOT] skipped (install matplotlib,pandas):", e)
        return

    plots_dir = os.path.join(out_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    skills_path = os.path.join(out_dir, "top_skills_by_country.csv")
    jt_path = os.path.join(out_dir, "jobtype_by_country.csv")
    metrics_path = os.path.join(out_dir, "run_metrics.json")

    # 1) Top countries by volume (from metrics)
    if os.path.exists(metrics_path):
        with open(metrics_path, "r", encoding="utf-8") as f:
            m = json.load(f)
        top = m.get("top_countries_by_volume", [])
        if top:
            s = pd.Series({k: v for k, v in top})
            plt.figure()
            s.plot(kind="bar")
            plt.title("Top countries by streamed volume")
            plt.tight_layout()
            p = os.path.join(plots_dir, "top_countries.png")
            plt.savefig(p, dpi=200)
            plt.close()
            print("[PLOT]", p)

    # 2) Overall job types
    if os.path.exists(jt_path):
        jt = pd.read_csv(jt_path)
        overall = jt.groupby("job_type")["count"].sum().sort_values(ascending=False).head(10)
        plt.figure()
        overall.plot(kind="bar")
        plt.title("Top job types (overall)")
        plt.tight_layout()
        p = os.path.join(plots_dir, "top_job_types.png")
        plt.savefig(p, dpi=200)
        plt.close()
        print("[PLOT]", p)

    # 3) Overall skills
    if os.path.exists(skills_path):
        sk = pd.read_csv(skills_path)
        overall = sk.groupby("skill")["count"].sum().sort_values(ascending=False).head(15)
        plt.figure()
        overall.plot(kind="bar")
        plt.title("Top skills (overall)")
        plt.tight_layout()
        p = os.path.join(plots_dir, "top_skills_overall.png")
        plt.savefig(p, dpi=200)
        plt.close()
        print("[PLOT]", p)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "jobs_raw"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP", "jobs-analytics-group"))

    parser.add_argument("--out-dir", default=OUT_DIR_DEFAULT)
    parser.add_argument("--top-n", type=int, default=30)

    parser.add_argument("--commit-every", type=int, default=5000)
    parser.add_argument("--idle-seconds", type=int, default=25)
    parser.add_argument("--log-every", type=int, default=20000)

    parser.add_argument("--exact", action="store_true", help="No pruning (uses more RAM)")
    parser.add_argument("--prune-mult", type=int, default=10)

    parser.add_argument("--snapshot-every", type=int, default=0, help="Write partial CSV every N msgs (0=off)")
    parser.add_argument("--plot", action="store_true", help="Save plots to data/plots (needs matplotlib)")
    args = parser.parse_args()

    skill_by_country: Dict[str, Counter] = defaultdict(Counter)
    jobtype_by_country: Dict[str, Counter] = defaultdict(Counter)
    country_count: Counter = Counter()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,   # manual commit
        group_id=args.group_id,
        max_poll_records=1000,
        request_timeout_ms=30000,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000,
    )

    processed = 0
    t0 = time.time()
    last_msg_time = time.time()

    print(f"[CONSUMER] topic={args.topic} | bootstrap={args.bootstrap} | group={args.group_id}")
    print("[CONSUMER] polling...")

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            got_any = False

            for _, msgs in records.items():
                if not msgs:
                    continue
                got_any = True

                for m in msgs:
                    job = m.value
                    country = norm(job.get("country"))
                    jt = norm(job.get("job_type"))
                    skills = job.get("skills") or []

                    country_count[country] += 1
                    jobtype_by_country[country][jt] += 1

                    for s in skills:
                        s2 = norm(s)
                        if s2 != "Unknown":
                            skill_by_country[country][s2] += 1

                    processed += 1

                    if args.log_every and processed % args.log_every == 0:
                        dt = max(time.time() - t0, 1e-6)
                        print(f"[CONSUMER] processed={processed:,} | {processed/dt:,.0f} msg/s")

                    if processed % args.commit_every == 0:
                        consumer.commit()

                    # pruning to keep memory small
                    if (not args.exact) and processed % (args.commit_every * 2) == 0:
                        keep = max(args.top_n * args.prune_mult, args.top_n)
                        for ctry, counter in skill_by_country.items():
                            if len(counter) > keep:
                                prune_counter(counter, keep)

                    if args.snapshot_every and processed % args.snapshot_every == 0:
                        print("[SNAPSHOT] writing partial outputs...")
                        write_outputs(args.out_dir, args.top_n, skill_by_country, jobtype_by_country)

            if got_any:
                last_msg_time = time.time()
            else:
                if time.time() - last_msg_time >= args.idle_seconds:
                    print(f"[CONSUMER] idle {args.idle_seconds}s -> stop")
                    break

        # final commit
        try:
            consumer.commit()
        except Exception:
            pass

    finally:
        consumer.close()

        # final outputs
        write_outputs(args.out_dir, args.top_n, skill_by_country, jobtype_by_country)

        metrics = {
            "topic": args.topic,
            "group_id": args.group_id,
            "bootstrap": args.bootstrap,
            "processed": processed,
            "runtime_sec": round(time.time() - t0, 3),
            "countries_seen": len(country_count),
            "top_countries_by_volume": country_count.most_common(10),
            "top_n": args.top_n,
            "commit_every": args.commit_every,
            "idle_seconds": args.idle_seconds,
            "timestamp": int(time.time()),
        }
        metrics_path = os.path.join(args.out_dir, "run_metrics.json")
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2)

        print("[METRICS]", metrics_path)
        print(f"[DONE] processed={processed:,}")

        if args.plot:
            try_plot(args.out_dir)


if __name__ == "__main__":
    main()
