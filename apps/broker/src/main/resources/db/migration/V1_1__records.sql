create table if not exists records (
    topic text not null,
    sequence bigint not null,
    producer_identity text not null,
    record_key text not null,
    idempotency_key text not null,
    correlation_id text not null,
    payload bytea null,
    published_at timestamptz not null,
    constraint records_topic_sequence_pk primary key (topic, sequence),
    constraint records_idempotency_uk unique (topic, producer_identity, idempotency_key)
);