"""
The simulated data (deterministic). Pure data, no Kafka/Kafi logic.
Each scene says WHAT is written to WHICH topic. The producer plays them back.
"""
from config import H, THRESHOLD_STORES


def ts(h, m=0, day=0):
    """Event time in ms since day 0, 00:00."""
    return day * 24 * H + (h * 60 + m) * 60 * 1000


def store(sid, name, region, tour, revenue):
    return {"key": sid, "value": {"store_id": sid, "name": name, "region": region,
                                  "tour_id": tour, "revenue": revenue}}


def tour(tid, dc, planned, status):
    return {"key": tid, "value": {"tour_id": tid, "dc": dc, "planned": planned, "status": status}}


def build_scenes():
    rid = iter(range(1, 10_000))

    def report(sid, text, h, m, day=0):
        i = next(rid)
        return {"key": str(i), "value": {"rid": i, "store_id": sid, "text": text, "ts": ts(h, m, day)}}

    return [
        dict(time="04:00", title="Load master data",
             story="Store and tour master data arrive as a changelog. No reports yet.",
             stores=[store("win", "Winterthur", "East",    "T-EAST-1", 18400),
                     store("frf", "Frauenfeld", "East",    "T-EAST-1", 12900),
                     store("wil", "Wil",        "East",    "T-EAST-2", 11300),
                     store("sg",  "St. Gallen", "East",    "T-EAST-2", 21700),
                     store("uzw", "Uzwil",      "East",    "T-EAST-3",  8900),
                     store("olt", "Olten",      "Central", "T-CEN-1",  14200),
                     store("aar", "Aarau",      "Central", "T-CEN-1",  15800)],
             tours=[tour("T-EAST-1", "DC Gossau", "05:40", "in_transit"),
                    tour("T-EAST-2", "DC Gossau", "05:50", "in_transit"),
                    tour("T-EAST-3", "DC Gossau", "06:10", "in_transit"),
                    tour("T-CEN-1", "DC Spreitenbach", "05:30", "in_transit")]),

        dict(time="05:10", title="Normal operation: noise, no alarm",
             story="Harmless reports, in two languages. Jev classifies them, the system stays calm.",
             reports=[report("olt", "Delivery 10 min late, no problem", 5, 10),
                      report("sg",  "Pallet arrived, all ok", 5, 11),
                      report("aar", "Camion en retard de 5 minutes, ça va", 5, 12)]),

        dict(time="05:45", title="The quiet start",
             story="Two stores in region East report problems, one in Central a breakdown. "
                   "Each report on its own is unremarkable. Below the threshold: no incident, no money at risk.",
             reports=[report("win", "Yoghurt pallet missing, driver says traffic jam on the A1", 5, 45),
                      report("frf", "Delivery not here yet, driver reports an accident on the A1", 5, 46),
                      report("olt", "Cooling unit broken, driver is turning back", 5, 47)]),

        dict(time="06:00", title="TIPPING POINT: pattern detected",
             story=f"The third store in region East reports the same cause ({THRESHOLD_STORES} = threshold). "
                   "An incident opens, 'value at risk' jumps, dispatch is alerted.",
             reports=[report("wil", "Pallet of yoghurt not here, driver says traffic jam on the motorway", 6, 0)]),

        dict(time="06:20", title="The incident grows (no second alarm)",
             story="Two more stores report the same. The existing incident is updated, "
                   "dispatch gets NO second message (idempotent).",
             reports=[report("sg",  "Camion bloccato in coda, ancora niente", 6, 20),
                      report("uzw", "Delivery missing, driver is stuck in traffic", 6, 21)]),

        dict(time="06:40", title="TWIST: master data changes mid-stream",
             story="Tour T-EAST-1 is reported as delivered (the driver found a detour). Incident, stores and "
                   "figure correct themselves in the same step: no intermediate values, no new alarm, "
                   "not a single new Jev call.",
             tours=[tour("T-EAST-1", "DC Gossau", "05:40", "delivered")]),

        dict(time="Day 2, 08:00", title="Cleanup: expiry, no GC code",
             story="24h later a routine report arrives. Yesterday's reports expire, and all joins and "
                   "aggregations behind them clean up on their own. Expiry is defined only once.",
             reports=[report("olt", "Pallet arrived, all ok", 8, 0, day=1)]),
    ]
