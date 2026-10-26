"""Central configuration: topics, business parameters and the storage factory (Kafka or local emulation)."""
import os
import sys

from kafi.kafi import Cluster, Local

# ── Topics ────────────────────────────────────────────────────────────────────
TOPIC_STORES = "fresh-stores"     # store master data (changelog, key = store_id)
TOPIC_TOURS = "fresh-tours"       # delivery tour master data (changelog, key = tour_id)
TOPIC_REPORTS = "fresh-reports"   # free-text reports from stores (append-only)
TOPICS = [TOPIC_STORES, TOPIC_TOURS, TOPIC_REPORTS]

# ── Business parameters ───────────────────────────────────────────────────────
H = 3600 * 1000                 # 1 hour in ms
EXPIRY_MS = 24 * H              # reports live for 24h (used exactly ONCE in the topology)
THRESHOLD_STORES = 3            # this many stores (same region + cause) = disruption incident
STORE_OPENING = "07:00"


# ── Storage ───────────────────────────────────────────────────────────────────
def get_storage():
    """
    DEMO_STORAGE=kafka (default)  -> real Kafka broker, KAFKA_BOOTSTRAP (default localhost:9092)
    DEMO_STORAGE=local            -> Kafi's file-system emulation (same API, no broker needed)
    """
    if storage_kind() == "local":
        storage = Local({"local": {"root.dir": os.environ.get("DEMO_LOCAL_DIR", "/tmp/fresh-demo")}})
    else:
        storage = Cluster({"kafka": {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")}})
    if hasattr(storage, "consume_timeout"):    # Kafka only: Kafi's default is 5s per poll and per source,
        storage.consume_timeout(0.2)           # which makes the demo sluggish. 0.2s = effects visible in < 1s
    return storage


def storage_kind():
    return os.environ.get("DEMO_STORAGE", "kafka").lower()


def storage_label():
    if storage_kind() == "local":
        return f"local emulation ({os.environ.get('DEMO_LOCAL_DIR', '/tmp/fresh-demo')})"
    return f"Kafka @ {os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092')}"


def require_broker(timeout=5.0):
    """Pre-flight: abort with a clear message if no broker is running (otherwise librdkafka spams errors forever)."""
    if storage_kind() != "kafka":
        return
    from confluent_kafka.admin import AdminClient
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    try:
        AdminClient({"bootstrap.servers": bootstrap, "log_level": 0}).list_topics(timeout=timeout)
    except Exception as e:
        sys.exit(f"\n✗ Kafka not reachable at {bootstrap} ({type(e).__name__}).\n"
                 f"  Is the broker running?  ->  docker compose up -d   (wait until 'healthy')\n"
                 f"  To test without Kafka: export DEMO_STORAGE=local\n")
