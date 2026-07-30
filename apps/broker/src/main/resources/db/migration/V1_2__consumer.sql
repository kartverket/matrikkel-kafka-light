create table if not exists consumer_offsets
(
    topic text not null check (char_length(topic) <= 128),
    consumer_group text not null,
    committed_sequence bigint not null,
    constraint consumer_offsets_topic_group_uk unique (topic, consumer_group)
);

create table if not exists consumer_leases
(
    topic text not null check (char_length(topic) <= 128),
    consumer_group text not null,
    instance_id text default '',
    token text default '',
    expires_at timestamptz default now(),
    constraint consumer_leases_topic_group_uk unique (topic, consumer_group),
    constraint consumer_leases_token_uk unique (token)
);
