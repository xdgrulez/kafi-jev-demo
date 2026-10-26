#!/usr/bin/env python3
"""
The data producer: a separate process that writes the simulated data to the Kafka topics.
Knows nothing about Kafi Streams, Jev or DuckDB.

  python producer.py setup               # create topics from scratch (before starting the processor!)
  python producer.py play                # play the scenes, wait for Enter after each one
  python producer.py play --auto 4       # next scene every 4 seconds
"""
import argparse
import time

from config import (TOPIC_REPORTS, TOPIC_STORES, TOPIC_TOURS, TOPICS, get_storage,
                    require_broker, storage_label)
from scenario import build_scenes

SCENE_TOPICS = (("stores", TOPIC_STORES), ("tours", TOPIC_TOURS), ("reports", TOPIC_REPORTS))


def setup(storage):
    for topic in TOPICS:
        storage.recreate(topic)
        print(f"  Topic '{topic}' recreated")


def play(storage, auto):
    for scene in build_scenes():
        print(f"\n▶ {scene['time']}  {scene['title']}")
        print(f"  {scene['story']}")
        for key, topic in SCENE_TOPICS:
            messages = scene.get(key)
            if not messages:
                continue
            producer = storage.producer(topic)
            producer.produce_list(messages)
            producer.close()                      # close() flushes
            print(f"  → {len(messages):>2} message(s) to '{topic}'")
        if auto:
            time.sleep(auto)
        else:
            input("\n  [Enter] next scene … ")
    print("\nDone. (Stop the processor with Ctrl+C to see the DuckDB report.)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", help="(re)create topics")
    p = sub.add_parser("play", help="play the scenes")
    p.add_argument("--auto", type=float, default=0, metavar="SEC", help="advance automatically after SEC seconds")
    args = ap.parse_args()

    print(f"Producer · {storage_label()}")
    require_broker()
    storage = get_storage()
    setup(storage) if args.cmd == "setup" else play(storage, args.auto)


if __name__ == "__main__":
    main()
