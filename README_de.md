# Frische-Lieferstörungen in Echtzeit: Kafi Streams + Jev + Kafka

Demo für Kafi Streams, modular und mit echtem Kafka. **Alle Daten und Zahlen sind simuliert.**

## Der Use Case auf einer Seite

**Die Ausgangslage.** Jede Nacht und jeden Morgen beliefern Verteilzentren die Filialen auf Touren mit Frischeware
(Joghurt, Beeren, Salate). Ist eine Tour spät, sind die Regale zur Ladenöffnung (07:00) leer oder die Ware kommt
zu warm an. Die Signale sind verstreut: Ein Fahrer erwähnt einen Stau, eine Mitarbeiterin tippt eine
Freitext-Meldung, und jemand in der Disposition muss erkennen, dass drei solche Meldungen aus drei Filialen
eigentlich *ein* Problem sind. Meist passiert das erst, wenn die Regale schon leer sind.

**Die Frage, die die Demo beantwortet.**
*Wie viel Umsatz ist gerade in Gefahr, wo und warum? Und lässt sich die Disposition alarmieren, sobald sich ein
Muster zeigt, statt erst nach dem Schaden?*

**Drei Eingänge**

| Eingang | Art | Beispiel |
|---|---|---|
| Filialstamm | Changelog (Updates ersetzen den alten Stand) | Winterthur, Region Ost, Tour T-OST-1, CHF 18'400 Frischeumsatz/Tag |
| Tourenstamm | Changelog | T-OST-1, ab VZ Gossau, geplant 05:40, Status `unterwegs` |
| Filialmeldungen | append-only Events, **Freitext** | "Joghurt-Palette fehlt, Fahrer sagt Stau A1" |

**Was die Pipeline tut**

1. **Join** jeder Meldung mit ihrer Filiale und ihrer Tour (so kennt die Meldung Region, Umsatz und geplante Ankunft).
2. **Klassifikation** des Freitexts mit Jev: die *Ursache* (`verkehr`, `fahrzeug`, `lieferant`, `keine_stoerung`)
   und die Wahrscheinlichkeit, dass die Lieferung die Ladenöffnung verpasst. Die Antworten sind typisierte Werte
   und lassen sich wie jede andere Spalte weiterverwenden.
3. **Filter** auf Meldungen, bei denen die Lieferung gefährdet ist *und* die Tour noch nicht zugestellt wurde.
4. **Aggregation:** betroffene Filialen pro Region und Ursache zählen. Ab 3 Filialen ist es ein **Störungsfall**.
   Ein verspäteter Lastwagen ist Rauschen, mehrere Filialen einer Region mit gleicher Ursache sind ein Muster.
5. **Aktion:** Frischeumsatz der betroffenen Filialen zum **Wert im Risiko** summieren, die Disposition einmal pro
   Fall alarmieren und alles in DuckDB festhalten.

**Beispiel-Durchlauf** (genau das zeigt `producer.py play`; alle Zahlen sind simuliert)

| Zeit | Was ankommt | Was passiert | Wert im Risiko |
|---|---|---|---|
| 04:00 | Filial- und Tourenstamm | Gespeichert. Noch keine Meldungen, nichts anzuzeigen. | CHF 0 |
| 05:10 | 3 harmlose Meldungen ("10 Min später, kein Problem", "Palette angekommen, alles ok", "Camion en retard de 5 minutes, ça va") | Jev stuft alle als `keine_stoerung` ein. Das System bleibt ruhig. | CHF 0 |
| 05:45 | Winterthur und Frauenfeld melden Stau bzw. Unfall, Olten ein defektes Kühlaggregat | Zwei Filialen Ost mit Ursache `verkehr` liegen unter der Schwelle von 3, Olten ist allein in Mitte. Noch kein Fall. | CHF 0 |
| **06:00** | **Wil** meldet dasselbe Verkehrsproblem | Dritte Filiale, gleiche Region, gleiche Ursache: **Fall öffnet sich**. Die Disposition bekommt eine Meldung mit den 3 Filialen. | **CHF 42'600** |
| 06:20 | St. Gallen (auf Italienisch!) und Uzwil melden dasselbe | Fall wächst von 3 auf 5 Filialen. **Keine zweite Meldung** (die Aktion ist idempotent). | CHF 73'200 |
| **06:40** | *Keine neue Meldung.* Tour T-OST-1 wird als `zugestellt` markiert (der Fahrer fand einen Umweg) | Winterthur und Frauenfeld fallen heraus. Der Fall schrumpft auf 3 Filialen, der Wert sinkt genau um deren Umsatz. **Kein neuer Alarm, kein neuer Jev-Call.** | **CHF 41'900** |
| Tag 2, 08:00 | Eine Routine-Meldung | Die Meldungen von gestern sind abgelaufen. Der Fall löst sich von selbst auf. | CHF 0 |

Drei Momente zum Zeigen in einer Live-Demo:
- **06:00:** mehrere unauffällige Meldungen werden zu einer Entscheidung.
- **06:40:** eine Stammdatenänderung (keine Meldung!) korrigiert Fall und Zahl in einem konsistenten Schritt.
  Niemand hat einen Job neu gestartet.
- **Tag 2:** der State verschwindet von selbst, weil die Expiry einmal an der Quelle deklariert wurde.

## Warum Stream Processing statt Data Warehouse?

Das ist kein "Warehouses sind schlecht". Der Punkt ist: Diese Frage hat eine **Deadline von Minuten**, und das
verändert, welches Werkzeug passt.

1. **Die Antwort verfällt.** Um 06:00 zu wissen, dass drei Filialen die Öffnung um 07:00 verpassen, ist viel wert.
   Es im Bericht von morgen zu lesen, nicht. Ein nächtlicher oder stündlicher Batch erzählt, was schon passiert
   ist. Micro-Batches im Warehouse alle paar Minuten verkleinern die Lücke, kosten aber dauernde Neuläufe, und man
   wartet trotzdem auf den nächsten Zyklus.
2. **Arbeit folgt den Änderungen, nicht der Historie.** Kafi (auf DBSP) verarbeitet nur, was sich geändert hat.
   Eine neue Meldung berührt eine Handvoll Zeilen, die Meldungen des letzten Tages werden nicht neu
   gerechnet. Warehouses können auch inkrementelle Sichten, aber mit Einschränkungen, und meist laufen sie nach
   Zeitplan.
3. **Die KI wird einmal pro Ereignis aufgerufen, wenn es passiert.** Jede Meldung wird klassifiziert, sobald sie
   eintrifft, und Jevs Antwort (Ursache, Wahrscheinlichkeit) wird einfach zur Spalte für die Joins und
   Aggregationen danach. In einem Batch-Setup muss man die Logik "welche Zeilen sind neu, welche habe ich schon
   klassifiziert?" selbst bauen.
4. **Die Aktion feuert, wenn die Bedingung wahr wird.** Ein Warehouse ist pull-basiert: Ein Dashboard oder Alert
   prüft nach Zeitplan, und man braucht Scheduler und Deduplizierung, damit die Disposition nicht jeden Zyklus
   alarmiert wird. Hier kommt der Alarm um 06:00, einmal, und nicht nochmal um 06:20.
5. **Korrekturen laufen konsistent durch.** Um 06:40 hat sich nur ein Tourenstatus geändert, und Fall,
   betroffene Filialen und CHF-Zahl passen sich im selben Schritt an, nie halb aktualisiert. Jeder Schritt liefert
   Fall, Filialen und Kennzahl gemeinsam aus einem Zeitpunkt.
6. **State hat einen Lebenszyklus, einmal deklariert.** Die Expiry ist eine Zeile an der Quelle, alle Joins und
   Aggregationen dahinter räumen sich selbst auf. Im Warehouse sind das Retention-Jobs, Partition Pruning und
   Housekeeping.
7. **Ein Programm statt fünf Werkzeuge.** Der entsprechende Warehouse-Stack besteht aus Ingestion, geplanten
   SQL-Transformationen, einem separaten Aufruf eines KI-Dienstes, einem Scheduler, einem Alerting-Tool und
   Deduplizierung. Diese Demo ist ein Python-Prozess: Join, Klassifikation, Aggregation und Aktion sind normaler
   Code an einer Stelle.

**Wo ein Warehouse weiterhin das richtige Werkzeug ist.** Jahre an Historie, Ad-hoc-Exploration, Reporting auf
Finanz-Niveau, Trainingsdaten für Modelle. Beides ergänzt sich: Der Stream *entscheidet jetzt*, das Warehouse
*analysiert später*. DuckDB in dieser Demo steht für diese Übergabe. In Produktion könnte der Sink genauso gut in
Snowflake, BigQuery oder Postgres schreiben.

**Ehrliche Einschränkungen.** Die Demo läuft mit einer Handvoll simulierter Datensätze. Die Argumente zu Aktualität
und Einfachheit sind architektonisch, keine hier gemessenen Werte; es gibt keinen Durchsatz- oder Kosten-Benchmark.
Der Offline-Modus ist eine Keyword-Heuristik und kein Modell.

## Architektur

```
 producer.py ──► Kafka ──► processor.py ─────────────────────────────────────────┐
 (eigener        frische-stores   │  topology.py   Kafi: Join, Group By, Expiry   │
  Prozess)       frische-tours    │  jev.py        Klassifikation (Choice, Noul)  │
                 frische-reports  │  results.py    Batch -> Konsole/DB/Aktion     │
                                  │  store.py      DuckDB (Embedded)              │
                                  │  actions.py    Dispo-Alarm (Stub)             │
                                  └───────────────────────────────────────────────┘
```

| Datei | Verantwortung | Kennt nicht |
|---|---|---|
| `producer.py` + `scenario.py` | simulierte Daten auf Topics schreiben | Kafi Streams, Jev, DuckDB |
| `topology.py` | die Stream-Logik (Quellen, Joins, Aggregation, Expiry) | DB, Aktionen |
| `jev.py` | Klassifikation + Cache | Kafka, Kafi, DuckDB |
| `store.py` | DuckDB-Schema, Writes, SQL-Report | Kafka, Kafi, Jev |
| `results.py` + `actions.py` | Ergebnis-Batch -> Anzeige, DB-Writes, Aktion | Topologie-Interna |
| `processor.py` | verdrahtet alles, läuft bis Ctrl+C | |
| `config.py` | Topics, Parameter, Storage-Fabrik | |

Die Topologie führt alle Ergebnisse über **einen** Sink zusammen. So bekommt `results.py` pro Schritt Fall,
betroffene Filialen und Kennzahl gemeinsam und kann nie einen Fall und eine Zahl aus verschiedenen
Zeitpunkten mischen.

## Start (3 Terminals)

```bash
pip install -r requirements.txt

# Terminal 0: Kafka (einmalig)
docker compose up -d

# Terminal 1: Topics frisch anlegen, dann den Processor starten
python producer.py setup
python processor.py --offline            # ohne API-Key. Mit Jev: export TYPESAFE_API_KEY=... und ohne --offline

# Terminal 2: Daten abspielen (Enter pro Szene, oder --auto 4 für automatisch)
python producer.py play
```

Reihenfolge: erst `setup`, dann Processor, dann `play`. Der Processor liest jedes Mal ab Topic-Anfang.
Ohne `setup` würde er alte Szenen vom letzten Lauf sofort wieder abspielen.

Ctrl+C im Processor beendet ihn und zeigt den DuckDB-Report. Mit `--db demo.duckdb` bleibt die Datei erhalten
(DuckDB erlaubt nur einen Schreiber, abfragbar erst nach dem Beenden).

### Ohne Docker / Kafka
`DEMO_STORAGE=local` nutzt Kafis Dateisystem-Emulation (gleiche API, selber Code, kein Broker). In jedem
Terminal setzen: `export DEMO_STORAGE=local`. Andere Broker-Adresse: `KAFKA_BOOTSTRAP=host:9092`.

## Jev
Zwei Fragen pro Meldung, mit Kontext (Filiale, Region, geplante Ankunft): `Choice` für die Ursache,
`Noul` (Wahrscheinlichkeit) für "Lieferung gefährdet?".

**Cache ist Pflicht:** Kafi wendet `map()` bei Retractions erneut auf alte Datensätze an
(Stammdatenänderung, Expiry). Ohne Cache kostet jede Änderung neue Jev-Calls.
Die Demo zeigt die Zahl: 10 Klassifikationen bei 23 `map`-Aufrufen.

## Annahmen und Grenzen
- "Wert im Risiko" = Frische-Tagesumsatz der Filialen in einem Störungsfall: grobe Vereinfachung.
- Der Tourenstatus ist bewusst NICHT im Jev-Kontext, sondern Relationenlogik (sonst Re-Klassifikation bei jeder Änderung).
- Kein Checkpointing: nach einem Neustart liest der Processor die Topics neu von vorn.
- Die Dispo-Meldung ist ein `print`. Dort käme Webhook/Mail/Ticket (oder ein Kafka-Topic) hin.
- **Getestet:** Producer + Processor als getrennte Prozesse mit `DEMO_STORAGE=local`. **Nicht getestet:** gegen einen
  echten Broker und gegen die echte Jev-API (in der Entwicklungsumgebung nicht verfügbar).
