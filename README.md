# Frische-Lieferstörungen in Echtzeit: Kafi Streams + Jev

Demo für Kafi Streams. **Alle Daten und Zahlen sind simuliert.**

```bash
pip install -r requirements.txt
python demo.py --offline     # ohne API-Key (Keyword-Stand-in, KEIN Modell)
export TYPESAFE_API_KEY=...  # dann: python demo.py   -> Jev live
python demo.py --pause       # für die Moderation: Enter pro Szene
```

## Pipeline

```
stores (Changelog) ─┐
tours  (Changelog) ─┼─ join ─ Jev (Choice + Noul) ─ filter(offen) ─┬─ group_by (Filiale) ─ count ─ filter(Schwelle) = Störungsfall ─ Aktion
reports (append-only, expire 24h) ─┘                              └─ ... ─ join ─ sum = Wert im Risiko              ─ DuckDB
```

| Kafi-Feature | Wo in der Demo |
|---|---|
| Expiry einmal ganz oben | `reports.expire(...)`, nichts weiter nötig |
| Join Stream + Changelog | Meldung ⨝ Filiale ⨝ Tour |
| Group By + Aggregation, zweistufig | Filiale → Störungsfall → Wert im Risiko |
| Strong Consistency | Szene 06:40: `(+neu, −alt)` im selben Schritt, nie ein Zwischenwert |
| Aktion + Embedded-DB | Dispo-Meldung (Stub) + DuckDB-Tabellen |

## Jev
Zwei Fragen pro Meldung, gestellt mit Kontext (Filiale, Region, geplante Ankunft):
`Choice` für die Ursache, `Noul` (Wahrscheinlichkeit) für "Lieferung gefährdet?".

**Cache ist Pflicht:** Kafi wendet `map()` bei Retractions erneut auf alte Datensätze an
(Stammdatenänderung, Expiry). Ohne Cache kostet jede Änderung neue Jev-Calls.
Die Demo zeigt die Zahl: 10 Klassifikationen bei 23 `map`-Aufrufen.

## Annahmen und Grenzen
- "Wert im Risiko" = Frische-Tagesumsatz der Filialen in einem Störungsfall. Das ist eine grobe Vereinfachung.
- Der Tourenstatus ist bewusst NICHT im Jev-Kontext, sondern reine Relationenlogik (sonst Re-Klassifikation bei jeder Statusänderung).
- Jev live ist hier nicht gegen die echte API getestet (kein Key). Geprüft: Fragen-Serialisierung gegen das SDK, Fallback bei Verbindungsfehler.
- Die Dispo-Meldung ist ein `print`. Dort käme Webhook/Mail/Ticket hin.
