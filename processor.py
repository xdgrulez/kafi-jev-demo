#!/usr/bin/env python3
"""
The stream processor: a separate process. Wires up topology (Kafi), classification (Jev),
embedded DB (DuckDB) and action, and runs until Ctrl+C. Then the DuckDB report appears.

  python processor.py                  # live Jev if TYPESAFE_API_KEY is set, otherwise offline simulation
  python processor.py --offline        # force the offline simulation
  python processor.py --db demo.duckdb # write DuckDB to a file (queryable with the duckdb CLI after Ctrl+C)
"""
import argparse
import logging
import signal
import time

from config import TOPICS, get_storage, require_broker, storage_label
from jev import make_classifier
from kafi.streams.streams import Streams
from results import ResultHandler
from store import ResultStore
from topology import build_topology


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--offline", action="store_true", help="offline simulation instead of Jev")
    ap.add_argument("--db", default=":memory:", help="DuckDB file (default: in memory)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)

    require_broker()
    storage = get_storage()
    for topic in TOPICS:                       # non-destructive: only create what is missing
        if not storage.exists(topic):
            storage.create(topic)

    classify, label = make_classifier(args.offline)
    store = ResultStore(args.db)
    handler = ResultHandler(store, classify)
    built_tn = build_topology(storage, classify, on_batch=handler)

    print("═" * 78)
    print(" Fresh-food delivery disruptions in real time · Kafi Streams + Jev")
    print(f" Storage:        {storage_label()}")
    print(f" Classification: {label}")
    print(" ALL DATA AND FIGURES ARE SIMULATED")
    print("═" * 78)
    print("\n Waiting for data … (Ctrl+C stops and shows the DuckDB report)", flush=True)

    signal.signal(signal.SIGTERM, signal.default_int_handler)   # treat SIGTERM like Ctrl+C
    stop = Streams.start_streams(built_tn, step_fun=handler.on_offsets)
    try:
        while Streams.threads():
            time.sleep(0.2)
        print("\n ✗ The Streams thread ended unexpectedly (see the log above for details).")
    except KeyboardInterrupt:
        print("\n Shutting down …")
    finally:
        stop()

    store.print_report()
    print(f"\n {len(handler.seen)} reports · {classify.kafi_calls} map calls by Kafi (incl. retractions) · "
          f"{classify.backend_calls} classifications by {label.split(' (')[0]}")
    print(" without the cache, every master data change would have cost new calls.\n")
    store.close()


if __name__ == "__main__":
    main()
