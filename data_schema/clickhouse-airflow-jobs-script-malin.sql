--简单的分布式建本地表语句 第二个cluster是在remote_servers配置的集群标签名
--是一些集群分片和副本的配置
--建表之前需要在每一个节点上创建数据库
--cluster的分片和副本规则
--<cluster>
--            <shard>
--                <weight>1</weight>
--                <internal_replication>true</internal_replication>
--                <replica>
--                    <host>ch-cluster-0-0</host>
--                    <port>9000</port>
--                </replica>
--                <replica>
--                    <host>ch-cluster-0-1</host>
--                    <port>9000</port>
--                </replica>
--            </shard>
--            <shard>
--                <weight>1</weight>
--                <internal_replication>true</internal_replication>
--                <replica>
--                    <host>ch-cluster-1-0</host>
--                    <port>9000</port>
--                </replica>
--                <replica>
--                    <host>ch-cluster-1-1</host>
--                    <port>9000</port>
--                </replica>
--            </shard>
--        </cluster>
create table oss_know.domo1 on cluster cluster(
 id UInt64
)
Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_1','{replica}')
order by id;
--***************************************************************************
--构建distributed表
--distributed表没有实体表
create table oss_know.domo1_all on cluster cluster(
 id UInt64
)
Engine = Distributed(cluster,oss_know,demo1,rand());

--简简单单向分布式表中插入数据
INSERT into domo2_all (*) values (12) (11) (10) (9) (8) (7) (6);


--opensearch 转移到 clickhouse 分布式表的构建
-- oss_know.base_git_1 definition

CREATE TABLE oss_know.base_git_1
(

    `create_time` DateTime64(3),

    `owner` String,

    `repo` String,

    `origin` String,

    `sha` String,

    `message` String,

    `author_tz` Int8,

    `committer_tz` Int8,

    `author_name` String,

    `author_email` String,

    `github_author_email` Array(String),

    `committer_name` String,

    `committer_email` String,

    `github_committer_email` Array(String),

    `authored_date` DateTime64(3),

    `authored_timestamp` UInt64,

    `committed_date` DateTime64(3),

    `committed_timestamp` UInt64,

    `github_author_profile.login` Array(String),

    `github_author_profile.id` Array(Int64),

    `github_author_profile.node_id` Array(String),

    `github_author_profile.avatar_url` Array(String),

    `github_author_profile.gravatar_id` Array(String),

    `github_author_profile.url` Array(String),

    `github_author_profile.html_url` Array(String),

    `github_author_profile.followers_url` Array(String),

    `github_author_profile.following_url` Array(String),

    `github_author_profile.gists_url` Array(String),

    `github_author_profile.starred_url` Array(String),

    `github_author_profile.subscriptions_url` Array(String),

    `github_author_profile.organizations_url` Array(String),

    `github_author_profile.repos_url` Array(String),

    `github_author_profile.events_url` Array(String),

    `github_author_profile.received_events_url` Array(String),

    `github_author_profile.github_type` Array(String),

    `github_author_profile.site_admin` Array(UInt8),

    `github_author_profile.name` Array(String),

    `github_author_profile.company` Array(String),

    `github_author_profile.blog` Array(String),

    `github_author_profile.location` Array(String),

    `github_author_profile.email` Array(String),

    `github_author_profile.hireable` Array(String),

    `github_author_profile.bio` Array(String),

    `github_author_profile.twitter_username` Array(String),

    `github_author_profile.public_repos` Array(Int32),

    `github_author_profile.public_gists` Array(Int32),

    `github_author_profile.github_followers` Array(Int32),

    `github_author_profile.github_following` Array(Int32),

    `github_author_profile.created_at` Array(DateTime64(3)),

    `github_author_profile.updated_at` Array(DateTime64(3)),

    `github_committer_profile.login` Array(String),

    `github_committer_profile.id` Array(Int64),

    `github_committer_profile.node_id` Array(String),

    `github_committer_profile.avatar_url` Array(String),

    `github_committer_profile.gravatar_id` Array(String),

    `github_committer_profile.url` Array(String),

    `github_committer_profile.html_url` Array(String),

    `github_committer_profile.followers_url` Array(String),

    `github_committer_profile.following_url` Array(String),

    `github_committer_profile.gists_url` Array(String),

    `github_committer_profile.starred_url` Array(String),

    `github_committer_profile.subscriptions_url` Array(String),

    `github_committer_profile.organizations_url` Array(String),

    `github_committer_profile.repos_url` Array(String),

    `github_committer_profile.events_url` Array(String),

    `github_committer_profile.received_events_url` Array(String),

    `github_committer_profile.github_type` Array(String),

    `github_committer_profile.site_admin` Array(UInt8),

    `github_committer_profile.name` Array(String),

    `github_committer_profile.company` Array(String),

    `github_committer_profile.blog` Array(String),

    `github_committer_profile.location` Array(String),

    `github_committer_profile.email` Array(String),

    `github_committer_profile.hireable` Array(String),

    `github_committer_profile.bio` Array(String),

    `github_committer_profile.twitter_username` Array(String),

    `github_committer_profile.public_repos` Array(Int32),

    `github_committer_profile.public_gists` Array(Int32),

    `github_committer_profile.github_followers` Array(Int32),

    `github_committer_profile.github_following` Array(Int32),

    `github_committer_profile.created_at` Array(DateTime64(3)),

    `github_committer_profile.updated_at` Array(DateTime64(3)),

    `files_total.insertions` UInt32,

    `files_total.deletions` UInt32,

    `files_total.lines` UInt32,

    `files_total.files` UInt32,

    `files.file_path` Array(String),

    `files.file_insertions` Array(UInt32),

    `files.file_deletions` Array(UInt32),

    `files.file_lines` Array(UInt32)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_4',
 '{replica}')
PARTITION BY toYYYYMMDD(create_time)
ORDER BY committed_timestamp
SETTINGS index_granularity = 8192;


--构建distributed表
-- oss_know.base_git_1 definition

CREATE TABLE oss_know.base_git_1_all on cluster cluster
(

    `create_time` DateTime64(3),

    `owner` String,

    `repo` String,

    `origin` String,

    `sha` String,

    `message` String,

    `author_tz` Int8,

    `committer_tz` Int8,

    `author_name` String,

    `author_email` String,

    `github_author_email` Array(String),

    `committer_name` String,

    `committer_email` String,

    `github_committer_email` Array(String),

    `authored_date` DateTime64(3),

    `authored_timestamp` UInt64,

    `committed_date` DateTime64(3),

    `committed_timestamp` UInt64,

--    `parent` Array(String),

    `github_author_profile.login` Array(String),

    `github_author_profile.id` Array(Int64),

    `github_author_profile.node_id` Array(String),

    `github_author_profile.avatar_url` Array(String),

    `github_author_profile.gravatar_id` Array(String),

    `github_author_profile.url` Array(String),

    `github_author_profile.html_url` Array(String),

    `github_author_profile.followers_url` Array(String),

    `github_author_profile.following_url` Array(String),

    `github_author_profile.gists_url` Array(String),

    `github_author_profile.starred_url` Array(String),

    `github_author_profile.subscriptions_url` Array(String),

    `github_author_profile.organizations_url` Array(String),

    `github_author_profile.repos_url` Array(String),

    `github_author_profile.events_url` Array(String),

    `github_author_profile.received_events_url` Array(String),

    `github_author_profile.github_type` Array(String),

    `github_author_profile.site_admin` Array(UInt8),

    `github_author_profile.name` Array(String),

    `github_author_profile.company` Array(String),

    `github_author_profile.blog` Array(String),

    `github_author_profile.location` Array(String),

    `github_author_profile.email` Array(String),

    `github_author_profile.hireable` Array(String),

    `github_author_profile.bio` Array(String),

    `github_author_profile.twitter_username` Array(String),

    `github_author_profile.public_repos` Array(Int32),

    `github_author_profile.public_gists` Array(Int32),

    `github_author_profile.github_followers` Array(Int32),

    `github_author_profile.github_following` Array(Int32),

    `github_author_profile.created_at` Array(DateTime64(3)),

    `github_author_profile.updated_at` Array(DateTime64(3)),

    `github_committer_profile.login` Array(String),

    `github_committer_profile.id` Array(Int64),

    `github_committer_profile.node_id` Array(String),

    `github_committer_profile.avatar_url` Array(String),

    `github_committer_profile.gravatar_id` Array(String),

    `github_committer_profile.url` Array(String),

    `github_committer_profile.html_url` Array(String),

    `github_committer_profile.followers_url` Array(String),

    `github_committer_profile.following_url` Array(String),

    `github_committer_profile.gists_url` Array(String),

    `github_committer_profile.starred_url` Array(String),

    `github_committer_profile.subscriptions_url` Array(String),

    `github_committer_profile.organizations_url` Array(String),

    `github_committer_profile.repos_url` Array(String),

    `github_committer_profile.events_url` Array(String),

    `github_committer_profile.received_events_url` Array(String),

    `github_committer_profile.github_type` Array(String),

    `github_committer_profile.site_admin` Array(UInt8),

    `github_committer_profile.name` Array(String),

    `github_committer_profile.company` Array(String),

    `github_committer_profile.blog` Array(String),

    `github_committer_profile.location` Array(String),

    `github_committer_profile.email` Array(String),

    `github_committer_profile.hireable` Array(String),

    `github_committer_profile.bio` Array(String),

    `github_committer_profile.twitter_username` Array(String),

    `github_committer_profile.public_repos` Array(Int32),

    `github_committer_profile.public_gists` Array(Int32),

    `github_committer_profile.github_followers` Array(Int32),

    `github_committer_profile.github_following` Array(Int32),

    `github_committer_profile.created_at` Array(DateTime64(3)),

    `github_committer_profile.updated_at` Array(DateTime64(3)),

    `files_total.insertions` UInt32,

    `files_total.deletions` UInt32,

    `files_total.lines` UInt32,

    `files_total.files` UInt32,

    `files.file_path` Array(String),

    `files.file_insertions` Array(UInt32),

    `files.file_deletions` Array(UInt32),

    `files.file_lines` Array(UInt32)
)
ENGINE = Distributed(cluster,oss_know,base_git_1,rand());










