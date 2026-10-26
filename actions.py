"""The action an incident triggers. Swappable: a webhook / mail / ticket / Kafka topic would go here."""
from store import chf


def dispatch_alert(region, cause, store_names, value):
    """STUB: only prints the dispatch message. Called exactly once per newly opened incident."""
    print(f"\n   📣 ACTION → Dispatch: disruption incident in region {region}, cause '{cause}'")
    print(f"      {len(store_names)} stores affected ({', '.join(sorted(store_names))}); "
          f"value at risk {chf(value)}. Consider rerouting.")
