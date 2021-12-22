create table if not exists "airflow-jobs".opensearch_bulk_failed_data
(
    id        serial
        constraint opensearch_bulk_failed_data_pk
            primary key,
    owner     varchar not null,
    repo      varchar not null,
    bulk_data text    not null,
    type      varchar not null
);

create unique index if not exists opensearch_bulk_failed_data_id_uindex
    on "airflow-jobs".opensearch_bulk_failed_data (id);

create index if not exists opensearch_bulk_failed_data__index_owner_repo
    on "airflow-jobs".opensearch_bulk_failed_data (owner, repo);

create index if not exists opensearch_bulk_failed_data__index_type
    on "airflow-jobs".opensearch_bulk_failed_data (type);

