create table if not exists records
(
    topic             text        not null check (char_length(topic) <= 128),
    sequence          bigint      not null,
    producer_identity text        not null check (char_length(producer_identity) <= 255),
    record_key        bytea       not null check (length(record_key) <= 255),
    idempotency_key   text        not null check (char_length(idempotency_key) <= 255),
    correlation_id    uuid        not null,
    payload           bytea       null,
    published_at      timestamptz not null,
    constraint records_topic_sequence_pk primary key (topic, sequence),
    constraint records_idempotency_uk unique (topic, record_key, producer_identity, idempotency_key)
);