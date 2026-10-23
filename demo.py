#!/usr/bin/env python3
"""
Frische-Lieferstörungen in Echtzeit  --  Kafi Streams + Jev (TypeSafe AI)

Szenario (SIMULIERT, keine echten Daten):
  Filialmeldungen im Freitext + Filial-/Tourenstammdaten  ->  Join  ->  Jev-Klassifikation
  ->  Aggregation  ->  "Wert im Risiko"  ->  Aktion (Disposition) + Embedded-DB (DuckDB)

Start:
  python demo.py                 # Jev live, wenn TYPESAFE_API_KEY gesetzt ist, sonst Offline-Simulation
  python demo.py --offline       # Offline-Simulation erzwingen (Keyword-Heuristik, KEIN Modell)
  python demo.py --pause         # nach jeder Szene auf Enter warten (für die Live-Moderation)
"""
import argparse
import os
import sys

import duckdb
from kafi.streams.topologynode import TopologyNode as Tn

H = 3600 * 1000            # 1 Stunde in ms
EXPIRY_MS = 24 * H         # Meldungen leben 24h  --  EINMAL definiert, ganz oben in der Topologie
SCHWELLE_FILIALEN = 3      # ab so vielen betroffenen Filialen (gleiche Region + Ursache) = Störungsfall
LADENOEFFNUNG = "07:00"


# ════════════════════════════════════════════════════════════════════════════
# 1. KLASSIFIKATION: Jev (echt) oder Offline-Simulation (Stand-in, kein Modell)
# ════════════════════════════════════════════════════════════════════════════

URSACHEN = {
    "verkehr":        "Stau, Unfall, Sperrung oder sonstige Verkehrsbehinderung auf der Strasse",
    "fahrzeug":       "Panne oder Defekt am Lieferfahrzeug, z.B. Kühlaggregat",
    "lieferant":      "Ware fehlt, wurde nicht oder falsch kommissioniert / geliefert",
    "keine_stoerung": "Routine-Meldung, Lieferung ist da oder die Verspätung ist unbedeutend",
}


class RealJev:
    """Jev über das offizielle SDK (pip install typesafe-sdk, Env: TYPESAFE_API_KEY)."""
    engine = "jev"

    def __init__(self):
        from typesafe_sdk import Choice, Noul, TypeSafeClient
        self.client = TypeSafeClient()
        self.questions = {
            "ursache": Choice(
                instructions="Was ist laut Meldung die Ursache der Lieferstörung?",
                criteria=URSACHEN),
            "gefaehrdet": Noul(
                instructions=f"Ist die Frischelieferung dieser Filiale eventuell "
                             f"nicht rechtzeitig vor Ladenöffnung ({LADENOEFFNUNG}) da?"),
        }

    def __call__(self, state):
        resp = self.client.system_one(state=state, questions=self.questions)
        print(resp)
        return resp.choices["ursache"].choice, resp.nouls["gefaehrdet"].noul


class SimulatedJev:
    """Offline-Stand-in mit Keywords. KEIN Modell. Nur damit die Demo ohne API-Key läuft."""
    engine = "offline-sim"

    def __call__(self, state):
        t = state["meldung"].lower()
        has = lambda *ws: any(w in t for w in ws)
        if has("kühl", "aggregat", "panne", "en panne", "guasto"):
            ursache = "fahrzeug"
        elif has("stau", "unfall", "accident", "embouteillage", "coda", "bouchon", "sperrung"):
            ursache = "verkehr"
        elif has("fehlt", "nicht kommissioniert", "manque", "mancano"):
            ursache = "lieferant"
        else:
            ursache = "keine_stoerung"
        if ursache == "keine_stoerung" or has("alles ok", "angekommen", "eingetroffen", "ça va", "kein problem"):
            return "keine_stoerung", 0.04
        return ursache, 0.96


class ClassifierWithCache:
    """
    Wrapper mit Cache pro Meldung (rid).

    WARUM? Kafi/DBSP ist inkrementell: ändert sich z.B. der Tourenstatus, wird der alte Join-Datensatz
    retrahiert (-1) und der neue eingefügt (+1) -- und die map()-Funktion läuft für BEIDE. Ohne Cache
    würde jede Stammdatenänderung neue Jev-Calls kosten. Mit Cache: genau ein Call pro Meldung.
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
            ursache, p = self.backend(state)
            engine = self.backend.engine
        except Exception as e:                       # Live-Demo darf nicht am WLAN sterben
            if not self.warned:
                print(f"\n  ⚠  {self.backend.engine} nicht erreichbar ({type(e).__name__}) -> Fallback Offline-Simulation\n")
                self.warned = True
            ursache, p = self.fallback(state)
            engine = self.fallback.engine
        self.cache[rid] = (ursache, round(p, 2), engine)
        return self.cache[rid]


# ════════════════════════════════════════════════════════════════════════════
# 2. DIE TOPOLOGIE  --  das Herzstück der Demo
# ════════════════════════════════════════════════════════════════════════════

def build_topology(classify):
    # --- Stammdaten (Changelog): nur das jeweils letzte Update pro Key zählt -------------------
    stores = (Tn.source("stores").compact()
              .map(lambda r: r["value"]))                       # store_id, name, region, tour_id, umsatz
    tours = (Tn.source("tours").compact()
             .map(lambda r: r["value"]))                        # tour_id, vz, geplant, status

    # --- Meldungen (append-only): Expiry EINMAL hier, alles dahinter räumt sich selbst auf -----
    reports = (Tn.source("reports")
               .map(lambda r: r["value"])                       # rid, store_id, text, ts
               .expire(ts_fun=lambda r: r["ts"], expiry_fun=lambda ts: ts + EXPIRY_MS))

    # --- Join: Meldung + Filiale + Tour ---------------------------------------------------------
    enriched = (reports
        .join(stores, lambda l: l["store_id"], lambda r: r["store_id"],
              lambda l, r: {**l, "filiale": r["name"], "region": r["region"],
                            "tour_id": r["tour_id"], "umsatz": r["umsatz"]})
        .join(tours, lambda l: l["tour_id"], lambda r: r["tour_id"],
              lambda l, r: {**l, "geplant": r["geplant"], "status": r["status"]}))

    # --- Jev: klassifiziert die Meldung MIT Kontext (Filiale, Region, geplante Ankunft) ---------
    def classify_fun(r):
        state = {"meldung": r["text"], "filiale": r["filiale"], "region": r["region"],
                 "tour": r["tour_id"], "geplante_ankunft": r["geplant"], "ladenoeffnung": LADENOEFFNUNG}
        ursache, p, engine = classify(r["rid"], state)
        return {**r, "ursache": ursache, "p_gefaehrdet": p, "engine": engine,
                "hit": p >= 0.5 and ursache != "keine_stoerung"}
    classified = enriched.map(classify_fun)

    # --- Offene Gefährdung = Jev sagt "gefährdet" UND Tour ist noch nicht zugestellt ------------
    #     (Der Tourenstatus ist reine Relationenlogik; ändert er sich, korrigiert Kafi alles
    #      Nachgelagerte konsistent -- ohne dass Jev nochmal gefragt wird.)
    offen = classified.filter(lambda r: r["hit"] and r["status"] != "zugestellt")

    signale = offen.count(project_fun=lambda n: {"meldungen": n})

    # --- Stufe 1: pro (Region, Ursache, Filiale) -- mehrere Meldungen derselben Filiale = 1 Filiale
    je_filiale = offen.group_by_agg(
        key_fun=lambda r: (r["region"], r["ursache"], r["store_id"]),
        value_fun=lambda r: r,
        agg_fun=lambda a, r: {"n": a["n"] + 1, "name": r["filiale"], "umsatz": r["umsatz"]},
        agg_initial_any={"n": 0, "name": "", "umsatz": 0},
        project_fun=lambda k, a: {"region": k[0], "ursache": k[1], "store_id": k[2],
                                  "name": a["name"], "umsatz": a["umsatz"]})

    # --- Stufe 2: Störungsfall = genug Filialen mit gleicher Ursache in einer Region -----------
    faelle = (je_filiale
              .group_by_count(key_fun=lambda r: (r["region"], r["ursache"]),
                              project_fun=lambda k, n: {"region": k[0], "ursache": k[1], "filialen": n})
              .filter(lambda r: r["filialen"] >= SCHWELLE_FILIALEN))

    # --- Betroffene Filialen = nur die, die zu einem Störungsfall gehören ----------------------
    betroffen = je_filiale.join(
        faelle, lambda l: (l["region"], l["ursache"]), lambda r: (r["region"], r["ursache"]),
        lambda l, r: l)

    # --- Wert im Risiko: jede Filiale nur einmal (auch bei zwei Ursachen), dann Summe ----------
    wert = (betroffen
            .group_by_agg(key_fun=lambda r: r["store_id"], value_fun=lambda r: r["umsatz"],
                          agg_fun=lambda a, u: u, agg_initial_any=0,
                          project_fun=lambda k, u: {"store_id": k, "umsatz": u})
            .sum(lambda r: r["umsatz"], lambda s: {"wert": s}))

    tn = Tn.build(classified.sink("klassifiziert"), signale.sink("signale"), faelle.sink("faelle"),
                  betroffen.sink("betroffen"), wert.sink("wert"))
    tn.from_zSet(Tn._to_records)          # (record, weight)-Paare statt nur positiver Records
    return tn


# ════════════════════════════════════════════════════════════════════════════
# 3. DAS SZENARIO (deterministisch, simuliert)
# ════════════════════════════════════════════════════════════════════════════

def ts(h, m=0, day=0):
    return day * 24 * H + (h * 60 + m) * 60 * 1000

def store(sid, name, region, tour, umsatz):
    return {"key": sid, "value": {"store_id": sid, "name": name, "region": region,
                                  "tour_id": tour, "umsatz": umsatz}}

def tour(tid, vz, geplant, status):
    return {"key": tid, "value": {"tour_id": tid, "vz": vz, "geplant": geplant, "status": status}}

_rid = iter(range(1, 10_000))
def meldung(sid, text, h, m, day=0):
    return {"value": {"rid": next(_rid), "store_id": sid, "text": text, "ts": ts(h, m, day)}}

SCENES = [
    dict(zeit="04:00", titel="Stammdaten laden",
         story="Filial- und Tourenstamm kommen als Changelog. Noch keine Meldungen.",
         stores=[store("win", "Winterthur",   "Ost",   "T-OST-1", 18400),
                 store("frf", "Frauenfeld",   "Ost",   "T-OST-1", 12900),
                 store("wil", "Wil",          "Ost",   "T-OST-2", 11300),
                 store("sg",  "St. Gallen",   "Ost",   "T-OST-2", 21700),
                 store("uzw", "Uzwil",        "Ost",   "T-OST-3",  8900),
                 store("olt", "Olten",        "Mitte", "T-MIT-1", 14200),
                 store("aar", "Aarau",        "Mitte", "T-MIT-1", 15800)],
         tours=[tour("T-OST-1", "VZ Gossau", "05:40", "unterwegs"),
                tour("T-OST-2", "VZ Gossau", "05:50", "unterwegs"),
                tour("T-OST-3", "VZ Gossau", "06:10", "unterwegs"),
                tour("T-MIT-1", "VZ Spreitenbach", "05:30", "unterwegs")]),

    dict(zeit="05:10", titel="Normalbetrieb: Rauschen, kein Alarm",
         story="Harmlose Meldungen, in zwei Sprachen. Jev stuft sie ein, das System bleibt ruhig.",
         reports=[meldung("olt", "Lieferung 10 Min später, kein Problem", 5, 10),
                  meldung("sg",  "Palette angekommen, alles ok", 5, 11),
                  meldung("aar", "Camion en retard de 5 minutes, ça va", 5, 12)]),

    dict(zeit="05:45", titel="Der leise Beginn",
         story="Zwei Filialen in Region Ost melden Probleme, eine in Mitte eine Panne. "
               "Jede Meldung für sich ist unauffällig. Unter der Schwelle: kein Fall, kein Geld im Risiko.",
         reports=[meldung("win", "Joghurt-Palette fehlt, Fahrer sagt Stau A1", 5, 45),
                  meldung("frf", "Lieferung noch nicht da, laut Fahrer Unfall auf der A1", 5, 46),
                  meldung("olt", "Kühlaggregat defekt, Fahrer kehrt um", 5, 47)]),

    dict(zeit="06:00", titel="KIPPPUNKT: Muster erkannt",
         story=f"Die dritte Filiale in Region Ost meldet dieselbe Ursache ({SCHWELLE_FILIALEN} = Schwelle). "
               "Störungsfall öffnet sich, 'Wert im Risiko' springt, Disposition wird alarmiert.",
         reports=[meldung("wil", "Palette Joghurt nicht da, Fahrer sagt Stau Autobahn", 6, 0)]),

    dict(zeit="06:20", titel="Der Fall wächst (kein zweiter Alarm)",
         story="Zwei weitere Filialen melden dasselbe. Der bestehende Fall wird aktualisiert, "
               "die Disposition bekommt KEINE zweite Meldung (idempotent).",
         reports=[meldung("sg",  "Camion bloccato in coda, ancora niente", 6, 20),
                  meldung("uzw", "Lieferung fehlt, Fahrer steckt im Stau", 6, 21)]),

    dict(zeit="06:40", titel="TWIST: Stammdaten ändern sich mitten im Stream",
         story="Tour T-OST-1 wird als zugestellt gemeldet (Fahrer fand einen Umweg). "
               "Fall, Filialen und Zahl korrigieren sich im selben Schritt, ohne Zwischenwerte, "
         "ohne neuen Alarm und ohne einen einzigen neuen Jev-Call.",
         tours=[tour("T-OST-1", "VZ Gossau", "05:40", "zugestellt")]),

    dict(zeit="Tag 2, 08:00", titel="Aufräumen: Expiry, ohne GC-Code",
         story="24h später kommt eine Routine-Meldung. Die Meldungen von gestern laufen ab, und alle Joins "
               "und Aggregationen dahinter räumen sich von selbst auf. Wir haben die Expiry nur einmal definiert.",
         reports=[meldung("olt", "Palette angekommen, alles ok", 8, 0, day=1)]),
]


# ════════════════════════════════════════════════════════════════════════════
# 4. DRIVER: füttert die Topologie, setzt Aktionen um, schreibt in DuckDB
# ════════════════════════════════════════════════════════════════════════════

def chf(v):
    return "CHF " + f"{int(v):,}".replace(",", "'")

def hhmm(ms):
    m = (ms // 60000) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


class Demo:
    def __init__(self, tn, classify, db_path):
        self.tn, self.classify = tn, classify
        self.seen = set()              # bereits gesehene Meldungs-IDs
        self.active = {}               # (region, ursache) -> Anzahl Filialen
        self.members = {}              # (region, ursache, store_id) -> Filiale (nur Störungsfälle)
        self.dispatched = set()        # Dedup-Schlüssel der bereits ausgelösten Aktionen
        self.wert = 0
        self.signale = 0
        self.db = duckdb.connect(db_path)
        for ddl in (
            "CREATE OR REPLACE TABLE meldungen (rid INT PRIMARY KEY, zeit VARCHAR, filiale VARCHAR, text VARCHAR, "
            "ursache VARCHAR, p_gefaehrdet DOUBLE, engine VARCHAR)",
            "CREATE OR REPLACE TABLE faelle (region VARCHAR, ursache VARCHAR, filialen INT, PRIMARY KEY (region, ursache))",
            "CREATE OR REPLACE TABLE aktionen (szene VARCHAR, region VARCHAR, ursache VARCHAR, filialen INT, wert_im_risiko INT)",
            "CREATE OR REPLACE TABLE wert_verlauf (szene VARCHAR, wert_im_risiko INT)"):
            self.db.execute(ddl)

    def step(self, scene):
        payload = {"stores": scene.get("stores", []), "tours": scene.get("tours", []),
                   "reports": scene.get("reports", [])}
        out = self.tn.process(payload)

        # 1) Neue Klassifikationen -> Anzeige + DuckDB-Audit-Log (einmal pro Meldung)
        for r, w in sorted(out["klassifiziert"], key=lambda rw: rw[0]["rid"]):
            if w > 0 and r["rid"] not in self.seen:
                self.seen.add(r["rid"])
                mark = "⚠ " if r["hit"] else "· "
                print(f"   {mark}{r['filiale']:<11} \"{r['text']}\"")
                print(f"        → {r['engine']}: ursache={r['ursache']}, gefährdet={r['p_gefaehrdet']:.2f}")
                self.db.execute("INSERT OR IGNORE INTO meldungen VALUES (?,?,?,?,?,?,?)",
                                [r["rid"], hhmm(r["ts"]), r["filiale"], r["text"],
                                 r["ursache"], r["p_gefaehrdet"], r["engine"]])

        # 2) Kennzahlen: (neu +1, alt -1) kommen im SELBEN Schritt -> nie ein Zwischenwert
        old_wert = self.wert
        self.signale = self._current(out["signale"], "meldungen", self.signale)
        self.wert = self._current(out["wert"], "wert", self.wert)

        # 3) Betroffene Filialen und Fälle nachführen
        for r, w in out["betroffen"]:
            k = (r["region"], r["ursache"], r["store_id"])
            if w > 0:
                self.members[k] = r["name"]
            else:
                self.members.pop(k, None)
        opened, grown, closed = [], [], []
        pos = {(r["region"], r["ursache"]): r["filialen"] for r, w in out["faelle"] if w > 0}
        neg = {(r["region"], r["ursache"]): r["filialen"] for r, w in out["faelle"] if w < 0}
        for k, n in pos.items():
            (grown if k in self.active else opened).append((k, n, self.active.get(k)))
            self.active[k] = n
            self.db.execute("INSERT OR REPLACE INTO faelle VALUES (?,?,?)", [k[0], k[1], n])
        for k in neg:
            if k not in pos:
                closed.append(k)
                self.active.pop(k, None)
                self.dispatched.discard(k)
                self.db.execute("DELETE FROM faelle WHERE region=? AND ursache=?", [k[0], k[1]])

        # 4) Ausgabe + Aktion
        delta = self.wert - old_wert
        sign = "+" if delta > 0 else ""
        print(f"\n   ┌─ Offene gefährdende Meldungen: {self.signale:<3}  Störungsfälle: {len(self.active)}")
        print(f"   └─ WERT IM RISIKO: {chf(self.wert)}" + (f"   ({sign}{chf(delta)[4:]})" if delta else ""))
        for k, n in pos.items():
            if k in [o[0] for o in opened]:
                names = ", ".join(sorted(v for kk, v in self.members.items() if kk[:2] == k))
                self.dispatch(scene["zeit"], k, n, names)
        for k, n, prev in grown:
            if prev != n:
                print(f"   ↻ Fall {k[0]}/{k[1]} aktualisiert: {prev} → {n} Filialen (keine neue Dispo-Meldung)")
        for k in closed:
            print(f"   ✓ Fall {k[0]}/{k[1]} aufgelöst")
        self.db.execute("INSERT INTO wert_verlauf VALUES (?,?)", [scene["zeit"], self.wert])
        print(f"   · Klassifikationen bisher: {self.classify.backend_calls}"
              f"   (map-Aufrufe durch Kafi inkl. Retractions: {self.classify.kafi_calls})")

    @staticmethod
    def _current(rows, field, previous):
        pos = [r[field] for r, w in rows if w > 0]
        if pos:
            return pos[-1]
        return 0 if any(w < 0 for _, w in rows) else previous

    def dispatch(self, szene, key, n, names):
        """AKTION. Hier wäre der Webhook / die Mail / das Ticket an die Disposition. Dedup: ein Fall = eine Meldung."""
        if key in self.dispatched:
            return
        self.dispatched.add(key)
        print(f"\n   📣 AKTION → Disposition: Störungsfall Region {key[0]}, Ursache '{key[1]}'")
        print(f"      {n} Filialen betroffen ({names}); Wert im Risiko {chf(self.wert)}. Umlenkung prüfen.")
        self.db.execute("INSERT INTO aktionen VALUES (?,?,?,?,?)", [szene, key[0], key[1], n, self.wert])


# ════════════════════════════════════════════════════════════════════════════
# 5. MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true", help="Offline-Simulation statt Jev")
    ap.add_argument("--pause", action="store_true", help="nach jeder Szene auf Enter warten")
    ap.add_argument("--db", default=":memory:", help="DuckDB-Datei (Standard: im Speicher)")
    args = ap.parse_args()

    fallback = SimulatedJev()
    use_jev = not args.offline and bool(os.environ.get("TYPESAFE_API_KEY"))
    backend = RealJev() if use_jev else fallback
    classify = ClassifierWithCache(backend, fallback)
    label = "Jev (TypeSafe AI)" if use_jev else "OFFLINE-SIMULATION (Keyword-Heuristik, kein Modell)"

    print("═" * 78)
    print(" Frische-Lieferstörungen in Echtzeit · Kafi Streams + Jev")
    print(f" Klassifikation: {label}")
    print(" ALLE DATEN UND ZAHLEN SIND SIMULIERT")
    print("═" * 78)

    demo = Demo(build_topology(classify), classify, args.db)
    for scene in SCENES:
        print(f"\n▶ {scene['zeit']}  {scene['titel']}")
        print(f"  {scene['story']}\n")
        demo.step(scene)
        if args.pause:
            input("\n  [Enter] weiter … ")

    # --- Schluss: Die Embedded-DB ist normales SQL --------------------------------------------
    print("\n" + "═" * 78)
    print(" DuckDB (Embedded): Nachbereitung per SQL")
    print("═" * 78)
    print("\n Meldungen pro Ursache:")
    for row in demo.db.execute("SELECT ursache, count(*) n, round(avg(p_gefaehrdet),2) FROM meldungen "
                               "GROUP BY 1 ORDER BY 2 DESC").fetchall():
        print(f"   {row[0]:<15} {row[1]:>2} Meldungen   Ø gefährdet={row[2]}")
    print("\n Ausgelöste Aktionen:")
    for row in demo.db.execute("SELECT szene, region, ursache, filialen, wert_im_risiko FROM aktionen").fetchall():
        print(f"   {row[0]}  Region {row[1]}, {row[2]}, {row[3]} Filialen, {chf(row[4])}")
    print("\n Wert im Risiko im Verlauf:")
    for row in demo.db.execute("SELECT szene, wert_im_risiko FROM wert_verlauf").fetchall():
        print(f"   {row[0]:<14} {chf(row[1]):>14}  " + "█" * int(row[1] / 3000))

    n = len(demo.seen)
    print(f"\n {n} Meldungen · {classify.kafi_calls} map-Aufrufe durch Kafi (inkl. Retractions) · "
          f"{classify.backend_calls} Klassifikationen durch {label.split(' (')[0]}")
    print(f" ohne Cache hätte jede Stammdatenänderung neue Calls gekostet.\n")


if __name__ == "__main__":
    sys.exit(main())
