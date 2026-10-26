# Fresh-food delivery disruptions in real time: Kafi Streams + Jev + Kafka

A demo for Kafi Streams, modular and with real Kafka. **All data and figures are simulated.**

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
