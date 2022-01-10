-- dev_oss.github_issues -------------------------
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

-- dev_oss.github_issues -------------------------
create table dev_oss.github_issues
(
    id           UInt64,
    create_time  Date,
    owner_id     String,
    owner        String,
    repo_id      String,
    repo         String,
    parent       Array(String),
    git_commit Nested(
        author Nested(
            name String,
            email String,
            date DateTime),
        committer Nested(
            name String,
            email String,
            date DateTime),
        message String,
        tree Nested( sha String,
            url String),
        url String,
        comment_count UInt32,
        verification Nested( verified String,
            reason String,
            signature String,
            payload String)
        ),
    url          String,
    html_url     String,
    comments_url String,
    author Nested(
        login String,
        id UInt64),
    committer Nested(
        login String,
        id UInt64)
) ENGINE = MergeTree()
      PARTITION BY toYYYYMMDD(create_time)
      ORDER BY (id)
      SETTINGS index_granularity = 4;



-- 目前测试通过的最复杂clickhouse sql ##########################################################
create table demo_git
(
    id                       UInt64,
    create_time              DateTime64(3),
    parent                   Array(String),
    `github_author.email`    String,
    `github_author.name`     String,
    `github_committer.email` String,
    `github_committer.name`  String,
    `files.insertions`       UInt32,
    `files.deletions`        UInt32,
    `files.lines`            UInt32,
    `files.stats.name`       Array(String),
    `files.stats.insertions` Array(UInt32),
    `files.stats.deletions`  Array(UInt32),
    `files.stats.lines`      Array(UInt64)
)
    engine = MergeTree PARTITION BY toYYYYMMDD(create_time)
        ORDER BY id
        SETTINGS index_granularity = 4;

INSERT INTO oss_know.demo_git (create_time, parent,
                               `github_author.email`, `github_author.name`,
                               `github_committer.email`, `github_committer.name`,
                               `files.insertions`, `files.deletions`, `files.lines`,
                               `files.stats.name`, `files.stats.insertions`, `files.stats.deletions`,
                               `files.stats.lines`)
VALUES ('2022-01-09 15:02:45.000', ['parent1','parent2'],
        'fivestarsky@163.com', 'fivestarsky', 'fivestarsky@163.com', 'fivestarsky', 100, 5, 105,
        ['readme.MD','test.py'], [60,40], [2,3], [62,43]);


select `files.stats.name`, `files.stats.insertions`, `files.stats.deletions`, `files.stats.lines`
from demo_git
where `github_committer.name` = 'fivestarsky';

SELECT `github_author.email`,
       `github_author.name`,
       `github_committer.email`,
       `github_committer.name`,
       `filesstats.name`,
       `filesstats.insertions`,
       `filesstats.deletions`,
       `filesstats.lines`
from demo_git
         ARRAY JOIN `files.stats` AS `filesstats`
WHERE files.stats.insertions[indexOf(files.stats.name, 'readme.MD')] > 10;

