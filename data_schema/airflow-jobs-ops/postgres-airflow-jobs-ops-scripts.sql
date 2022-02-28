create table if not exists retry_data
(
    id               serial
        constraint retry_data_pk
            primary key,
    owner            varchar,
    repo             varchar,
    data             varchar,
    type             varchar,
    create_timestamp date default CURRENT_TIMESTAMP
);

create unique index if not exists retry_data_id_uindex
    on retry_data (id);

create index if not exists retry_data_owner_repo_index
    on retry_data (owner, repo);

create index if not exists retry_data_type_index
    on retry_data (type);

