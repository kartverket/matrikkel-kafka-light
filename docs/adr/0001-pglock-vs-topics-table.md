# ADR: Sikring av monootont økende sekvensnummer per topic

- Status: Accepted
- Date: 2026-06-10

## Kontekst

Meldingstjenesten må tildele monotont økende sekvensnummer per topic.

Flere publiseringer til samme topic kan skje samtidig. Sekvenstildeling må derfor koordineres slik at to transaksjoner
ikke tildeler samme neste sekvensnummer.

Topic-konfigurasjon defineres i kode.

## Beslutning

Vi bruker `pg_advisory_xact_lock` for å serialisere sekvenstildeling per topic.

Ved publisering tar tjenesten en transaksjonsscopet PostgreSQL advisory lock per topic. Deretter leses høyeste
eksisterende sekvens fra `records`, og ny melding får `max(sequence) + 1`.

Topic-eksistens avgjøres av in-code konfigurasjon.

For å finne høyeste nå-verdi for en gitt topic brukes følgende SQL:

```sql
select sequence
from records
where topic = :topic
order by sequence desc
limit 1
```

Denne formen gjør det tydelig at databasen kan bruke `(topic, sequence)`-indeksen baklengs og stoppe etter første rad.

## Konsekvenser

Positive konsekvenser:

- Databaseskjemaet blir mindre.
- Startup-rekonsiliering mellom kodekonfigurasjon og database trengs ikke.
- Topic-konfigurasjon har én tydelig kilde til sannhet.
- Sekvenstildeling er serialisert per topic.
- `records` er nok til å utlede topic head.

Negative konsekvenser:

- Publisering er avhengig av korrekt bruk av PostgreSQL advisory locks.
- Fremtidig cleanup må aldri slette raden med høyeste sekvens for et topic, siden neste sekvens utledes fra beholdte
  records.
- Advisory lock-nøkkelen må beregnes stabilt og med lav kollisjonsrisiko.

## Alternativer vurdert

### Bruke en `topics`-tabell

Dette alternativet ville lagret `current_head`, som viser siste sekvensnummer for en gitt topic, i en egen `topics`
-tabell.

Det ville gitt et enkelt låsepunkt og raskt head-oppslag, men også innført ekstra runtime-tilstand som måtte
rekonsilieres ved oppstart.

### Bruke `pg_advisory_xact_lock`

Dette alternativet bruker innebygd funksjonalitet i PostgreSQL for å ta en arbitrær transaksjonsscopet lås.

Det fjerner behovet for en `topics`-tabell og rekonsiliering ved oppstart, samt behovet for å oppdatere `current_head`
ved hver publisering.

## Resultat

Vi bruker PostgreSQL som koordineringsmekanisme via transaksjonsscopede advisory locks, og lar `records` være den
autoritative kilden for topic head.