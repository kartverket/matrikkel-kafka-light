create table if not exists topics (
    topic text primary key,
    active boolean not null,
    current_head bigint not null,
    updated_at timestamp not null
);