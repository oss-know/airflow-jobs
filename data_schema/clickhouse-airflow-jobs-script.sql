create table dev_oss.git
(
    id                  UInt64,
    create_time         Date,
    owner_id            String,
    owner               String,
    repo_id             String,
    repo                String,
    origin              String,
    type_id             UInt8,
    type                String,
    sha                 String,
    message             String,
    author_tz           Int8,
    committer_tz        Int8,
    author_name         String,
    author_email        String,
    committer_name      String,
    committer_email     String,
    authored_date       DateTime,
    authored_timestamp  UInt64,
    committed_date      DateTime,
    committed_timestamp UInt64,
    parent              Array(String),
    total Nested(insertions UInt32, deletions UInt32, lines UInt32, files UInt32),
    `files.file_name`   Array(String),
    `files.file_path`   Array(String),
    `files.stats`       Array(Nested(insertions UInt32, deletions UInt32, lines UInt64))
) ENGINE = MergeTree()
      PARTITION BY toYYYYMMDD(create_time)
      ORDER BY (id)
      SETTINGS index_granularity = 4;
