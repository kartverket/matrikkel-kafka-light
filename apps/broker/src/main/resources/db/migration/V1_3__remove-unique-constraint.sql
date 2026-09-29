-- Legger til tilsvarende index for å bevare raske oppslag på `idempotency_key`
CREATE INDEX records_batch_idempotency_idx
    ON records (topic, producer_identity, idempotency_key, sequence DESC);

-- Fjerner constraint siden det er gyldig at en batch inneholder samme `record_key` flere ganger
ALTER TABLE records
    DROP CONSTRAINT records_idempotency_uk;

