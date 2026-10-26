"""
The glue between topology and the outside world: receives one batch [(result, weight), ...] per processing
step and turns it into console output, DuckDB writes and actions.

The batch is a complete, consistent step: (new +1, old -1) of a KPI are always in the same batch,
so an intermediate value is never visible.
"""
from actions import dispatch_alert
from store import chf


def hhmm(ms):
    m = (ms // 60000) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


class ResultHandler:
    def __init__(self, store, classify):
        self.store, self.classify = store, classify
        self.step = 0
        self.seen = set()          # report IDs already seen
        self.active = {}           # (region, cause) -> number of stores
        self.members = {}          # (region, cause, store_id) -> store name (incident stores only)
        self.value = 0
        self.signals = 0
        self._last_offsets = {}
        self._ready = False

    # ── Hook: Streams calls this after every step (before the sinks), to display the input ──────
    def on_offsets(self, _, offsets):
        if not self._ready:                    # first loop iteration = consumers are connected
            self._ready = True
            print(" ✓ connected, ready for data\n", flush=True)
        delta = {}
        for source, partitions in offsets.items():
            total = sum(partitions.values())
            if total > self._last_offsets.get(source, 0):
                delta[source] = total - self._last_offsets.get(source, 0)
            self._last_offsets[source] = total
        if delta:                              # show input immediately, even if it doesn't (yet) change a result
            print("\n▸ Input: " + ", ".join(f"{s} +{n}" for s, n in sorted(delta.items())), flush=True)

    # ── Sink: one batch = one consistent step ─────────────────────────────────────────────────
    def __call__(self, batch):
        by_kind = {}
        for rec, w in batch:
            by_kind.setdefault(rec["kind"], []).append((rec["row"], w))

        self.step += 1
        new = [r for r, w in by_kind.get("classified", []) if w > 0 and r["rid"] not in self.seen]
        when = hhmm(max(r["ts"] for r in new)) if new else "–"
        print(f"── Result {self.step} · event time {when} " + "─" * 40)

        # 1) new classifications -> display + DuckDB audit log (once per report)
        for r in sorted(new, key=lambda r: r["rid"]):
            self.seen.add(r["rid"])
            print(f"   {'⚠ ' if r['hit'] else '· '}{r['store_name']:<11} \"{r['text']}\"")
            print(f"        → {r['engine']}: cause={r['cause']}, at_risk={r['p_at_risk']:.2f}")
            self.store.add_report(r["rid"], hhmm(r["ts"]), r["store_name"], r["text"],
                                  r["cause"], r["p_at_risk"], r["engine"])

        # 2) KPIs: (new +1, old -1) in the same batch
        old_value = self.value
        self.signals = self._current(by_kind.get("signals", []), "reports", self.signals)
        self.value = self._current(by_kind.get("value", []), "value", self.value)

        # 3) maintain affected stores and incidents
        for r, w in by_kind.get("affected", []):
            k = (r["region"], r["cause"], r["store_id"])
            if w > 0:
                self.members[k] = r["name"]
            else:
                self.members.pop(k, None)
        incidents = by_kind.get("incidents", [])
        pos = {(r["region"], r["cause"]): r["stores"] for r, w in incidents if w > 0}
        neg = {(r["region"], r["cause"]) for r, w in incidents if w < 0}
        opened = [k for k in pos if k not in self.active]
        grown = [(k, self.active[k], n) for k, n in pos.items() if k in self.active and self.active[k] != n]
        closed = [k for k in neg if k not in pos]
        for k, n in pos.items():
            self.active[k] = n
            self.store.upsert_incident(k[0], k[1], n)
        for k in closed:
            self.active.pop(k, None)
            self.store.delete_incident(k[0], k[1])

        # 4) display + action (the action sees incident, affected stores and KPI of the same step)
        delta = self.value - old_value
        print(f"\n   ┌─ Open at-risk reports: {self.signals:<3}  Incidents: {len(self.active)}")
        print(f"   └─ VALUE AT RISK: {chf(self.value)}"
              + (f"   ({'+' if delta > 0 else '-'}{chf(abs(delta))[4:]})" if delta else ""))
        for k in opened:
            names = [v for kk, v in self.members.items() if kk[:2] == k]
            dispatch_alert(k[0], k[1], names, self.value)
            self.store.add_action(self.step, k[0], k[1], pos[k], self.value)
        for k, before, now in grown:
            print(f"   ↻ Incident {k[0]}/{k[1]} updated: {before} → {now} stores (no new dispatch message)")
        for k in closed:
            print(f"   ✓ Incident {k[0]}/{k[1]} resolved")
        self.store.add_value(self.step, when, self.value)
        print(f"   · Classifications so far: {self.classify.backend_calls}"
              f"   (map calls by Kafi incl. retractions: {self.classify.kafi_calls})", flush=True)

    @staticmethod
    def _current(rows, field, previous):
        pos = [r[field] for r, w in rows if w > 0]
        if pos:
            return pos[-1]
        return 0 if any(w < 0 for _, w in rows) else previous
