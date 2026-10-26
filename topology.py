"""
The Kafi topology. Pure stream logic: sources -> join -> classification -> aggregation -> ONE result sink.
Knows no database and no action; results go to the supplied function `on_batch`.

All results (with weights +1/-1) flow through ONE sink. So on_batch receives everything of a processing
step together: incident, affected stores, KPI. An incident and a figure from different points in time
can therefore never be mixed.
"""
from config import (EXPIRY_MS, STORE_OPENING, THRESHOLD_STORES,
                    TOPIC_REPORTS, TOPIC_STORES, TOPIC_TOURS)
from kafi.streams.streams import Streams


def build_topology(storage, classify, on_batch):
    # --- Master data (changelog from Kafka): only the latest update per key counts ----------------
    stores = (Streams.source(storage, "stores", TOPIC_STORES).compact()
              .map(lambda r: r["value"]))                       # store_id, name, region, tour_id, revenue
    tours = (Streams.source(storage, "tours", TOPIC_TOURS).compact()
             .map(lambda r: r["value"]))                        # tour_id, dc, planned, status

    # --- Reports (append-only): expiry ONCE, right here; everything behind it cleans up by itself -
    reports = (Streams.source(storage, "reports", TOPIC_REPORTS)
               .map(lambda r: r["value"])                       # rid, store_id, text, ts
               .expire(ts_fun=lambda r: r["ts"], expiry_fun=lambda ts: ts + EXPIRY_MS))

    # --- Join: report + store + tour -------------------------------------------------------------
    enriched = (reports
        .join(stores, lambda l: l["store_id"], lambda r: r["store_id"],
              lambda l, r: {**l, "store_name": r["name"], "region": r["region"],
                            "tour_id": r["tour_id"], "revenue": r["revenue"]})
        .join(tours, lambda l: l["tour_id"], lambda r: r["tour_id"],
              lambda l, r: {**l, "planned": r["planned"], "status": r["status"]}))

    # --- Jev: classifies the report WITH context (store, region, planned arrival) ----------------
    def classify_fun(r):
        state = {"report": r["text"], "store": r["store_name"], "region": r["region"],
                 "tour": r["tour_id"], "planned_arrival": r["planned"], "store_opening": STORE_OPENING}
        cause, p, engine = classify(r["rid"], state)
        return {**r, "cause": cause, "p_at_risk": p, "engine": engine,
                "hit": p >= 0.5 and cause != "no_disruption"}
    classified = enriched.map(classify_fun)

    # --- Open risk = Jev says "at risk" AND the tour has not been delivered yet ------------------
    #     The tour status is pure relational logic: when it changes, Kafi corrects everything
    #     downstream consistently, without asking Jev again.
    open_risk = classified.filter(lambda r: r["hit"] and r["status"] != "delivered")

    signals = open_risk.count(project_fun=lambda n: {"reports": n})

    # --- Stage 1: per (region, cause, store): several reports from one store = 1 store ----------
    per_store = open_risk.group_by_agg(
        key_fun=lambda r: (r["region"], r["cause"], r["store_id"]),
        value_fun=lambda r: r,
        agg_fun=lambda a, r: {"n": a["n"] + 1, "name": r["store_name"], "revenue": r["revenue"]},
        agg_initial_any={"n": 0, "name": "", "revenue": 0},
        project_fun=lambda k, a: {"region": k[0], "cause": k[1], "store_id": k[2],
                                  "name": a["name"], "revenue": a["revenue"]})

    # --- Stage 2: incident = enough stores with the same cause in one region ---------------------
    incidents = (per_store
                 .group_by_count(key_fun=lambda r: (r["region"], r["cause"]),
                                 project_fun=lambda k, n: {"region": k[0], "cause": k[1], "stores": n})
                 .filter(lambda r: r["stores"] >= THRESHOLD_STORES))

    # --- Affected stores = only those that belong to an incident ---------------------------------
    affected = per_store.join(
        incidents, lambda l: (l["region"], l["cause"]), lambda r: (r["region"], r["cause"]),
        lambda l, r: l)

    # --- Value at risk: each store only once (even with two causes), then the sum ----------------
    value = (affected
             .group_by_agg(key_fun=lambda r: r["store_id"], value_fun=lambda r: r["revenue"],
                           agg_fun=lambda a, u: u, agg_initial_any=0,
                           project_fun=lambda k, u: {"store_id": k, "revenue": u})
             .sum(lambda r: r["revenue"], lambda s: {"value": s}))

    # --- ONE sink: merge all result streams, tagged (merge = Z-set addition, weights are kept) ---
    def tag(kind):
        return lambda r: {"kind": kind, "row": r}
    results = (classified.map(tag("classified"))
               .merge(signals.map(tag("signals")))
               .merge(incidents.map(tag("incidents")))
               .merge(affected.map(tag("affected")))
               .merge(value.map(tag("value"))))

    built_tn = Streams.build(results.sink_fun(on_batch, "results"))
    built_tn.from_zSet(Streams._to_records)     # (record, weight) pairs instead of positive records only
    return built_tn
