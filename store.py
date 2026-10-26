"""Embedded DB (DuckDB): audit log, current state and histories. Knows nothing about Kafka, Kafi or Jev."""
import duckdb


def chf(v):
    return "CHF " + f"{int(v):,}".replace(",", "'")


class ResultStore:
    def __init__(self, path=":memory:"):
        self.db = duckdb.connect(path)
        for ddl in (
            # audit log: every report exactly once, with the Jev result
            "CREATE OR REPLACE TABLE reports (rid INT PRIMARY KEY, event_time VARCHAR, store VARCHAR, "
            "text VARCHAR, cause VARCHAR, p_at_risk DOUBLE, engine VARCHAR)",
            # current state: open disruption incidents
            "CREATE OR REPLACE TABLE incidents (region VARCHAR, cause VARCHAR, stores INT, "
            "PRIMARY KEY (region, cause))",
            # triggered actions
            "CREATE OR REPLACE TABLE actions (step INT, region VARCHAR, cause VARCHAR, "
            "stores INT, value_at_risk INT)",
            # history of the KPI
            "CREATE OR REPLACE TABLE value_history (step INT, event_time VARCHAR, value_at_risk INT)"):
            self.db.execute(ddl)

    # ── Writing ───────────────────────────────────────────────────────────────
    def add_report(self, rid, event_time, store, text, cause, p, engine):
        self.db.execute("INSERT OR IGNORE INTO reports VALUES (?,?,?,?,?,?,?)",
                        [rid, event_time, store, text, cause, p, engine])

    def upsert_incident(self, region, cause, stores):
        self.db.execute("INSERT OR REPLACE INTO incidents VALUES (?,?,?)", [region, cause, stores])

    def delete_incident(self, region, cause):
        self.db.execute("DELETE FROM incidents WHERE region=? AND cause=?", [region, cause])

    def add_action(self, step, region, cause, stores, value):
        self.db.execute("INSERT INTO actions VALUES (?,?,?,?,?)", [step, region, cause, stores, value])

    def add_value(self, step, event_time, value):
        self.db.execute("INSERT INTO value_history VALUES (?,?,?)", [step, event_time, value])

    # ── Reading: post-processing with SQL ─────────────────────────────────────
    def print_report(self):
        q = lambda sql: self.db.execute(sql).fetchall()
        print("\n" + "═" * 78)
        print(" DuckDB (embedded): post-processing with SQL")
        print("═" * 78)
        print("\n Reports per cause:")
        for c, n, p in q("SELECT cause, count(*), round(avg(p_at_risk),2) FROM reports "
                         "GROUP BY 1 ORDER BY 2 DESC"):
            print(f"   {c:<15} {n:>2} reports   avg at-risk={p}")
        print("\n Triggered actions:")
        for s, region, c, n, w in q("SELECT step, region, cause, stores, value_at_risk FROM actions"):
            print(f"   Step {s}: region {region}, {c}, {n} stores, {chf(w)}")
        print("\n Value at risk over time:")
        for s, t, w in q("SELECT step, event_time, value_at_risk FROM value_history ORDER BY step"):
            print(f"   Step {s:<2} {t:<6} {chf(w):>14}  " + "█" * int(w / 3000))

    def close(self):
        self.db.close()
