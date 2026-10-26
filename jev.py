"""
Classification: Jev (TypeSafe AI) or an offline stand-in. Knows nothing about Kafka, Kafi or DuckDB.

Interface:  classify(rid, state) -> (cause, p_at_risk, engine)
"""
import os

from config import STORE_OPENING

CAUSES = {
    "traffic":       "Traffic jam, accident, road closure or other disruption on the road",
    "vehicle":       "Breakdown or defect on the delivery vehicle, e.g. the cooling unit",
    "supplier":      "Goods missing, not picked or wrongly picked / delivered",
    "no_disruption": "Routine report, delivery has arrived or the delay is insignificant",
}


class RealJev:
    """Jev via the official SDK (pip install typesafe-sdk, env: TYPESAFE_API_KEY)."""
    engine = "jev"

    def __init__(self):
        from typesafe_sdk import Choice, Noul, TypeSafeClient
        self.client = TypeSafeClient()
        self.questions = {
            "cause": Choice(
                instructions="According to the report, what is the cause of the delivery disruption?",
                criteria=CAUSES),
            "at_risk": Noul(
                instructions=f"Is this store's fresh-food delivery likely to miss store opening "
                             f"({STORE_OPENING})?"),
        }

    def __call__(self, state):
        resp = self.client.system_one(state=state, questions=self.questions)
        return resp.choices["cause"].choice, resp.nouls["at_risk"].noul


class SimulatedJev:
    """Offline stand-in using keywords. NOT a model. Only so the demo runs without an API key."""
    engine = "offline-sim"

    def __call__(self, state):
        t = state["report"].lower()
        has = lambda *ws: any(w in t for w in ws)
        if has("cooling", "refrigerat", "breakdown", "broke down", "en panne", "guasto"):
            cause = "vehicle"
        elif has("traffic", "jam", "accident", "crash", "road closed", "embouteillage", "bouchon", "coda"):
            cause = "traffic"
        elif has("missing", "not picked", "manque", "mancano"):
            cause = "supplier"
        else:
            cause = "no_disruption"
        if cause == "no_disruption" or has("all ok", "arrived", "no problem", "ça va"):
            return "no_disruption", 0.04
        return cause, 0.96


class ClassifierWithCache:
    """
    Cache per report (rid).

    WHY? Kafi/DBSP is incremental: when e.g. a tour status changes, the old join record is retracted (-1)
    and the new one inserted (+1), and the map() function runs for BOTH. Without a cache every master data
    change would cost new Jev calls. With the cache: exactly one call per report.
    """
    def __init__(self, backend, fallback):
        self.backend, self.fallback = backend, fallback
        self.cache, self.kafi_calls, self.backend_calls, self.warned = {}, 0, 0, False

    def __call__(self, rid, state):
        self.kafi_calls += 1
        if rid in self.cache:
            return self.cache[rid]
        self.backend_calls += 1
        try:
            cause, p = self.backend(state)
            engine = self.backend.engine
        except Exception as e:                       # a live demo must not die on bad wifi
            if not self.warned:
                print(f"\n  ⚠  {self.backend.engine} not reachable ({type(e).__name__}) "
                      f"-> falling back to offline simulation\n", flush=True)
                self.warned = True
            cause, p = self.fallback(state)
            engine = self.fallback.engine
        self.cache[rid] = (cause, round(p, 2), engine)
        return self.cache[rid]


def make_classifier(offline=False):
    """-> (classify, label). Live Jev if TYPESAFE_API_KEY is set and --offline was not requested."""
    fallback = SimulatedJev()
    use_jev = not offline and bool(os.environ.get("TYPESAFE_API_KEY"))
    backend = RealJev() if use_jev else fallback
    label = "Jev (TypeSafe AI)" if use_jev else "OFFLINE SIMULATION (keyword heuristic, not a model)"
    return ClassifierWithCache(backend, fallback), label
