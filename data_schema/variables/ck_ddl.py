# -*-coding:utf-8-*-
import json

ck_ddl = {"ck_create_table_ddl": [
    {
        "table_name": "email_main_tz_map",
        "local_table": """
        create table if not exists email_main_tz_map_local on cluster replicated
        (
            update_at           DateTime64(3),
            update_at_timestamp Int64,
            email               String,
            main_tz_area        String,
            top_n_tz_area Array(Tuple(String,Int64))
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/email_main_tz_map_', '{replica}')
        ORDER BY (email)
        SETTINGS index_granularity = 8192;
        """,
        "distributed_table": """
        create table if not exists email_main_tz_map on cluster replicated as email_main_tz_map_local
        engine = Distributed('replicated', 'default', 'email_main_tz_map_local', update_at_timestamp);
        """
    },
    {
        "table_name": "github_id_main_tz_map",
        "local_table": """
        create table if not exists github_id_main_tz_map_local on cluster replicated
        (
            update_at           DateTime64(3),
            update_at_timestamp Int64,
            github_id           Int64,
            main_tz_area        String,
            inferred_area       String,
            location            String,
            top_n_tz_area Array(Tuple(Array(String),String,Int64))
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/github_id_main_tz_map', '{replica}')
        ORDER BY (github_id)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists github_id_main_tz_map on cluster replicated as github_id_main_tz_map_local
        engine = Distributed('replicated', 'default', 'github_id_main_tz_map_local', update_at_timestamp);
       """
    },
    {
        "table_name": "github_id_approved_reviewed_commented_map",
        "local_table": """
        create table if not exists github_id_approved_reviewed_commented_map_local on cluster replicated
        (
            update_at           DateTime64(3),
            update_at_timestamp Int64,
            search_key__owner String,
            search_key__repo String,
            id           Int64,
            approved_count Int64,
            reviewed_commented Int64
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/github_id_approved_reviewed_commented_map_local', '{replica}')
        ORDER BY (search_key__owner,search_key__repo,id)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists github_id_approved_reviewed_commented_map on cluster replicated as github_id_approved_reviewed_commented_map_local
        engine = Distributed('replicated', 'default', 'github_id_approved_reviewed_commented_map_local', update_at_timestamp);
       """
    },
    {
        "table_name": "gits_dir_label",
        "local_table": """
        create table if not exists gits_dir_label_local on cluster replicated
        (
            ck_data_insert_at Int64,
            search_key__owner String,
            search_key__repo  String,
            author_tz         Int64,
            committer_tz      Int64,
            author_name       String,
            author_email      String,
            authored_date     DateTime64(3),
            committer_name    String,
            committer_email   String,
            committed_date    DateTime64(3),
            dir_list Array(String),
            array_slice Array(String),
            in_dir            String
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_label', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists gits_dir_label on cluster replicated as gits_dir_label_local
        engine = Distributed('replicated', 'default', 'gits_dir_label_local', ck_data_insert_at);
       """
    },
    {
        "table_name": "gits_dir",
        "local_table": """
        create table if not exists gits_dir_local  on cluster replicated
        (
            ck_data_insert_at Int64,
            search_key__owner String,
            search_key__repo  String,
            dir               String
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir', '{replica}')
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists gits_dir on cluster replicated as gits_dir_local
        engine = Distributed('replicated', 'default', 'gits_dir_local', ck_data_insert_at);
       """
    },
    {
        "table_name": "gits_dir_email_domain_contributer_count",
        "local_table": """
        create table if not exists gits_dir_email_domain_contributer_count_local on cluster replicated
        (
            ck_data_insert_at Int64,
            search_key__owner String,
            search_key__repo  String,
            in_dir            String,
            authored_date     Int64,
            email_domain      String,
            contributer_count Int64
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_email_domain_contributer_count_', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists gits_dir_email_domain_contributer_count on cluster replicated as gits_dir_email_domain_contributer_count_local
        engine = Distributed('replicated', 'default', 'gits_dir_email_domain_contributer_count_local', ck_data_insert_at);
       """
    },
    {
        "table_name": "gits_dir_email_domain_alter_file_count",
        "local_table": """
        create table if not exists gits_dir_email_domain_alter_file_count_local on cluster replicated
        (
            ck_data_insert_at Int64,
            search_key__owner String,
            search_key__repo  String,
            in_dir            String,
            authored_date     Int64,
            email_domain      String,
            alter_file_count  Int64
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_email_domain_alter_file_count_', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists gits_dir_email_domain_alter_file_count on cluster replicated as gits_dir_email_domain_alter_file_count_local
        engine = Distributed('replicated', 'default', 'gits_dir_email_domain_alter_file_count_local', ck_data_insert_at);
       """
    },
    {
        "table_name": "gits_dir_contributor_tz_distribution",
        "local_table": """
        create table if not exists gits_dir_contributor_tz_distribution_local on cluster replicated
        (
            ck_data_insert_at Int64,
            search_key__owner String,
            search_key__repo  String,
            in_dir            String,
            author_email      String,
            alter_files_count Int64,
            tz_distribution Array(Map(Int64, UInt64))
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_contributor_tz_distribution', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists gits_dir_contributor_tz_distribution  on cluster replicated as gits_dir_contributor_tz_distribution_local
        engine = Distributed('replicated', 'default', 'gits_dir_contributor_tz_distribution_local', ck_data_insert_at);
       """
    },
    {
        "table_name": "gits_alter_file_times",
        "local_table": """
        create table if not exists gits_alter_file_times_local on cluster replicated
        (
            ck_data_insert_at Int64,
            search_key__owner String,
            search_key__repo  String,
            in_dir            String,
            authored_date     Int64,
            area              String,
            alter_file_count  Int64
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_alter_file_times1', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists gits_alter_file_times on cluster replicated as gits_alter_file_times_local
        engine = Distributed('replicated', 'default', 'gits_alter_file_times_local', ck_data_insert_at);
       """
    },
    {
        "table_name": "gits_dir_contributer",
        "local_table": """
        create table if not exists gits_dir_contributer_local on cluster replicated
        (
            ck_data_insert_at Int64,
            search_key__owner String,
            search_key__repo  String,
            in_dir            String,
            authored_date     Int64,
            area              String,
            contributer_count Int64
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir_contributer_', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
       """,
        "distributed_table": """
        create table if not exists gits_dir_contributer on cluster replicated as gits_dir_contributer_local
        engine = Distributed('replicated', 'default', 'gits_dir_contributer_local', ck_data_insert_at);

       """
    },
    {

        "table_name": "country_tz_region_map",
        "local_table": """
         create table country_tz_region_map_local on cluster replicated
        (
            update_at DateTime64(3),
            country_or_region String,
            area String
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/country_tz_region_map_local', '{replica}')
        ORDER BY country_or_region
        SETTINGS index_granularity = 8192;
""",
        "distributed_table": """
         create table country_tz_region_map on cluster replicated as country_tz_region_map_local
         engine = Distributed('replicated', 'default', 'country_tz_region_map_local', rand());
"""
    },
    {

        "table_name": "country_tz_region_map",
        "local_table": """
         create table github_id_email_map_local on cluster replicated
        (
            update_at DateTime64(3),
            update_at_timestamp Int64,
            github_id Int64,
            email String
        )
        engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/github_id_email_map', '{replica}')
        ORDER BY github_id
        SETTINGS index_granularity = 8192;
""",
        "distributed_table": """
         create table github_id_email_map on cluster replicated as github_id_email_map_local
         engine = Distributed('replicated', 'default', 'github_id_email_map_local', rand());

"""
    }
]}
with open('ddl.json', 'w') as f:
    json.dump(ck_ddl, f)
