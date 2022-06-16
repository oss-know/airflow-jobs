-- gits_dir_label_local
create table gits_dir_label_local on cluster replicated
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

-- gits_dir_label
create table gits_dir_label on cluster replicated
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
    engine = Distributed('replicated', 'default', 'gits_dir_label_local', ck_data_insert_at);

--gits_alter_file_times_local
create table gits_alter_file_times_local
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

--gits_alter_file_times
create table gits_alter_file_times
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    authored_date     Int64,
    area              String,
    alter_file_count  Int64
)
    engine = Distributed('replicated', 'default', 'gits_alter_file_times_local', ck_data_insert_at);

--gits_dir_contributer_local
create table gits_dir_contributer_local
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

--gits_dir_contributer
create table gits_dir_contributer
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    authored_date     Int64,
    area              String,
    contributer_count Int64
)
    engine = Distributed('replicated', 'default', 'gits_dir_contributer_local', ck_data_insert_at);


--gits_dir_email_domain_alter_file_count_local
create table gits_dir_email_domain_alter_file_count_local
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

--gits_dir_email_domain_alter_file_count
create table gits_dir_email_domain_alter_file_count
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    authored_date     Int64,
    email_domain      String,
    alter_file_count  Int64
)
    engine = Distributed('replicated', 'default', 'gits_dir_email_domain_alter_file_count_local', ck_data_insert_at);

--gits_dir_email_domain_contributer_count_local
create table gits_dir_email_domain_contributer_count_local
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

--gits_dir_email_domain_contributer_count
create table gits_dir_email_domain_contributer_count
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    authored_date     Int64,
    email_domain      String,
    contributer_count Int64
)
    engine = Distributed('replicated', 'default', 'gits_dir_email_domain_contributer_count_local', ck_data_insert_at);

--gits_dir_contributor_tz_distribution_local
create table gits_dir_contributor_tz_distribution_local
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


--gits_dir_contributor_tz_distribution
create table gits_dir_contributor_tz_distribution
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    in_dir            String,
    author_email      String,
    alter_files_count Int64,
    tz_distribution Array(Map(Int64, UInt64))
)
    engine = Distributed('replicated', 'default', 'gits_dir_contributor_tz_distribution_local', ck_data_insert_at);

--gits_dir_local
create table gits_dir_local
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    dir               String
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/gits_dir', '{replica}')
        PARTITION BY search_key__owner
        ORDER BY (search_key__owner, search_key__repo)
        SETTINGS index_granularity = 8192;
--gits_dir
create table gits_dir
(
    ck_data_insert_at Int64,
    search_key__owner String,
    search_key__repo  String,
    dir               String
)
    engine = Distributed('replicated', 'default', 'gits_dir_local', ck_data_insert_at);