# Fresh-food delivery disruptions in real time: Kafi Streams + Jev + Kafka

A demo for Kafi Streams, modular and with real Kafka. **All data and figures are simulated.**

## The use case in one page

**The situation.** Every night and morning, distribution centers deliver fresh food (yoghurt, berries, salads) to
stores on delivery tours. When a tour is late, shelves are empty at opening time (07:00) or the goods arrive too
warm. The signals are scattered: a driver mentions a traffic jam, a store employee types a free-text message,
and somebody in dispatch has to notice that three such messages from three stores are really *one* problem.
That usually happens after the shelves are already empty.

**The question the demo answers.**
*How much revenue is at risk right now, where, and why? And can dispatch be alerted the moment a pattern
emerges, not after the damage is done?*

**Three inputs**

| Input | Kind | Example |
|---|---|---|
| Store master data | changelog (updates replace old state) | Winterthur, region East, tour T-EAST-1, CHF 18'400 fresh revenue/day |
| Tour master data | changelog | T-EAST-1, from DC Gossau, planned 05:40, status `in_transit` |
| Store reports | append-only events, **free text** | "Yoghurt pallet missing, driver says traffic jam on the A1" |

**What the pipeline does**

1. **Join** every report with its store and its tour (so a report knows its region, revenue and planned arrival).
2. **Classify** the free text with Jev: the *cause* (`traffic`, `vehicle`, `supplier`, `no_disruption`) and the
   probability that the delivery misses store opening. The answers are typed values, so they can be used like any
   other column.
3. **Filter** to reports where the delivery is at risk *and* the tour has not been delivered yet.
4. **Aggregate:** count the affected stores per region and cause. From 3 stores on, it is an **incident**.
   One late truck is noise; several stores in one region with the same cause is a pattern.
5. **Act:** sum the fresh revenue of the affected stores into **value at risk**, alert dispatch once per
   incident, and record everything in DuckDB.

**Example walkthrough** (this is exactly what `producer.py play` shows; all figures are simulated)

| Time | What arrives | What happens | Value at risk |
|---|---|---|---|
| 04:00 | Store and tour master data | Stored. No reports yet, nothing to show. | CHF 0 |
| 05:10 | 3 harmless reports ("10 min late, no problem", "Pallet arrived, all ok", "Camion en retard de 5 minutes, ça va") | Jev rates all as `no_disruption`. The system stays calm. | CHF 0 |
| 05:45 | Winterthur and Frauenfeld report a traffic jam / accident; Olten reports a broken cooling unit | Two East stores with cause `traffic` is below the threshold of 3, and Olten is alone in Central. No incident yet. | CHF 0 |
| **06:00** | **Wil** reports the same traffic problem | Third store, same region, same cause: **incident opens**. Dispatch gets one alert naming the 3 stores. | **CHF 42'600** |
| 06:20 | St. Gallen (in Italian!) and Uzwil report the same | Incident grows from 3 to 5 stores. **No second alert** (the action is idempotent). | CHF 73'200 |
| **06:40** | *No new report.* Tour T-EAST-1 is marked `delivered` (the driver found a detour) | Winterthur and Frauenfeld drop out. Incident shrinks to 3 stores, value falls by exactly their revenue. **No new alert, no new Jev call.** | **CHF 41'900** |
| Day 2, 08:00 | One routine report | Yesterday's reports have expired. The incident resolves itself. | CHF 0 |

Three moments to point at in a live demo:
- **06:00:** several unremarkable messages become one decision.
- **06:40:** a master data change (not a report!) corrects the incident and the figure in one consistent step.
  Nobody re-ran a job.
- **Day 2:** state disappears on its own, because expiry was declared once at the source.

## Why stream processing instead of a data warehouse?

This is not "warehouses are bad". The point is that this particular question has a **deadline of minutes**, and
that changes which tool fits.

1. **The answer expires.** Knowing at 06:00 that three stores will miss the 07:00 opening is valuable. Knowing it
   in tomorrow's report is not. A nightly or hourly batch job tells you what already happened. Micro-batching a
   warehouse every few minutes narrows the gap, but you pay for it with constant re-runs and still wait for the
   next cycle.
2. **Work follows the changes, not the history.** Kafi (built on DBSP) processes only what changed. A new report
   touches a handful of rows; the reports of the last day are not recomputed. Warehouses can do
   incremental views too, but with restrictions, and they usually refresh on a schedule.
3. **The AI is called once per event, when it happens.** Each report is classified the moment it arrives, so
   Jev's answer (a cause, a probability) simply becomes a column for the joins and aggregations behind it.
   In a batch setup you have to build the "which rows are new, which did I already classify?" logic yourself.
4. **The action fires when the condition becomes true.** A warehouse is pull-based: a dashboard or alert checks
   on a schedule, and you need a scheduler plus de-duplication so dispatch is not alerted every cycle. Here the
   alert happens at 06:00, once, and not again at 06:20.
5. **Corrections ripple through consistently.** At 06:40 only a tour status changed, and the incident, the
   affected stores and the CHF figure all adjust in the same step, never half-updated. Every step delivers
   incident, stores and KPI together from one point in time.
6. **State has a lifecycle, declared once.** Expiry is one line at the source; every join and aggregation behind
   it cleans up by itself. In a warehouse that is retention jobs, partition pruning and housekeeping.
7. **One program instead of five tools.** The equivalent warehouse stack is ingestion, scheduled SQL
   transformations, a separate call-out to an AI service, a scheduler, an alerting tool and de-duplication.
   This demo is one Python process: join, classification, aggregation and action are plain code in one place.

**Where a warehouse is still the right tool.** Years of history, ad-hoc exploration, finance-grade reporting,
training data for models. The two complement each other: the stream *decides now*, the warehouse *analyzes
later*. DuckDB in this demo stands for that hand-off. In production the sink could just as well write to
Snowflake, BigQuery or Postgres.

**Honest caveats.** The demo runs a handful of simulated records. The freshness and simplicity arguments above are
architectural, not measurements made here; there is no throughput or cost benchmark. The offline mode is a
keyword heuristic, not a model.

## Architecture

```
 producer.py ──► Kafka ──► processor.py ─────────────────────────────────────────┐
 (separate       fresh-stores    │  topology.py   Kafi: join, group by, expiry    │
  process)       fresh-tours     │  jev.py        classification (Choice, Noul)   │
                 fresh-reports   │  results.py    batch -> console / DB / action  │
                                 │  store.py      DuckDB (embedded)               │
                                 │  actions.py    dispatch alert (stub)           │
                                 └────────────────────────────────────────────────┘
```

| File | Responsibility | Knows nothing about |
|---|---|---|
| `producer.py` + `scenario.py` | write simulated data to the topics | Kafi Streams, Jev, DuckDB |
| `topology.py` | the stream logic (sources, joins, aggregation, expiry) | DB, actions |
| `jev.py` | classification + cache | Kafka, Kafi, DuckDB |
| `store.py` | DuckDB schema, writes, SQL report | Kafka, Kafi, Jev |
| `results.py` + `actions.py` | result batch -> display, DB writes, action | topology internals |
| `processor.py` | wires everything up, runs until Ctrl+C | |
| `config.py` | topics, parameters, storage factory | |

The topology routes all results through **one** sink. That way `results.py` receives incident, affected
stores and KPI together per step and can never mix an incident and a figure from different points in time.

## Run (3 terminals)

```bash
pip install -r requirements.txt

# Terminal 0: Kafka (once)
docker compose up -d

# Terminal 1: create topics from scratch, then start the processor
python producer.py setup
python processor.py --offline            # no API key. With Jev: export TYPESAFE_API_KEY=... and drop --offline

# Terminal 2: play the data (Enter per scene, or --auto 4 for hands-free)
python producer.py play
```

Order: first `setup`, then the processor, then `play`. The processor reads from the start of each topic every
time. Without `setup` it would instantly replay old scenes from the previous run.

Ctrl+C in the processor stops it and shows the DuckDB report. With `--db demo.duckdb` the file is kept
(DuckDB allows only one writer, so you can query it only after shutdown).

### Without Docker / Kafka
`DEMO_STORAGE=local` uses Kafi's file-system emulation (same API, same code, no broker). Set
`export DEMO_STORAGE=local` in every terminal. Different broker address: `KAFKA_BOOTSTRAP=host:9092`.

## Jev
Two questions per report, asked with context (store, region, planned arrival): `Choice` for the cause,
`Noul` (a probability) for "delivery at risk?".

**The cache is mandatory:** Kafi re-applies `map()` to old records on retractions (master data change,
expiry). Without a cache every change costs new Jev calls.
The demo shows the number: 10 classifications for 23 `map` calls.

The reports are deliberately multilingual (English and French/Italian), as in a Swiss retail setting.

## Assumptions and limits
- "Value at risk" = fresh-food daily revenue of the stores in an incident: a coarse simplification.
- The tour status is deliberately NOT part of the Jev context but relational logic (otherwise every change would re-classify).
- No checkpointing: after a restart the processor re-reads the topics from the beginning.
- The dispatch message is a `print`. A webhook/mail/ticket (or a Kafka topic) would go there.
- **Tested:** producer + processor as separate processes with `DEMO_STORAGE=local`. **Not tested:** against a
  real broker and the real Jev API (not available in the development environment).
