GITHUB_TOKENS = "github_tokens"
GIT_SAVE_LOCAL_PATH = "git_save_local_path"
OPENSEARCH_CONN_DATA = "opensearch_conn_data"

DAILY_SYNC_GITS_INCLUDES = "gits_sync_includes"
DAILY_SYNC_GITS_EXCLUDES = "gits_sync_excludes"
DAILY_SYNC_GITHUB_COMMITS_EXCLUDES = "github_commits_sync_excludes"
DAILY_SYNC_GITHUB_COMMITS_INCLUDES = "github_commits_sync_includes"
DAILY_SYNC_GITHUB_PRS_INCLUDES = "github_prs_sync_includes"
DAILY_SYNC_GITHUB_PRS_EXCLUDES = "github_prs_sync_excludes"
DAILY_SYNC_GITHUB_ISSUES_INCLUDES = "github_issues_sync_includes"
DAILY_SYNC_GITHUB_ISSUES_EXCLUDES = "github_issues_sync_excludes"
DAILY_SYNC_GITHUB_ISSUES_COMMENTS_INCLUDES = "github_issues_comments_sync_includes"
DAILY_SYNC_GITHUB_ISSUES_COMMENTS_EXCLUDES = "github_issues_comments_sync_excludes"
DAILY_SYNC_GITHUB_ISSUES_TIMELINE_INCLUDES = "github_issues_timeline_sync_includes"
DAILY_SYNC_GITHUB_ISSUES_TIMELINE_EXCLUDES = "github_issues_timeline_sync_excludes"

DAILY_SYNC_CLICKHOUSE_GITS_INCLUDES = "daily_sync_clickhouse_gits_includes"
DAILY_SYNC_CLICKHOUSE_GITHUB_COMMITS_INCLUDES = "daily_sync_clickhouse_github_commits_includes"
DAILY_SYNC_CLICKHOUSE_GITHUB_PRS_INCLUDES = "daily_sync_clickhouse_github_prs_includes"
DAILY_SYNC_CLICKHOUSE_GITHUB_ISSUES_INCLUDES = "daily_sync_clickhouse_github_issues_includes"

POSTGRES_CONN_INFO = "postgres_conn_info"
NEED_INIT_GITHUB_ISSUES_COMMENT_REACTION_REPOS = "need_init_github_issues_comments_reactions"
NEED_INIT_GITHUB_COMMITS_REPOS = "need_init_github_commits_repos"
NEED_INIT_GITHUB_ISSUES_REPOS = "need_init_github_issues_repos"
NEED_INIT_GITHUB_PULL_REQUESTS_REPOS = "need_init_github_pull_requests_repos"
NEED_INIT_GITHUB_ISSUES_TIMELINE_REPOS = "need_init_github_issues_timeline_repos"
NEED_INIT_GITHUB_ISSUES_COMMENTS_REPOS = "need_init_github_issues_comments_repos"
NEED_INIT_GITHUB_PROFILES_REPOS = "need_init_github_profiles_repos"
NEED_INIT_TRANSFER_TO_CLICKHOUSE = "need_init_transfer_to_clickhouse"
NEED_SYNC_GITHUB_PROFILES_REPOS = "need_sync_github_profiles_repos"
NEED_SYNC_GITHUB_ISSUES_REPOS = "need_sync_github_issues_repos"
NEED_SYNC_GITHUB_PULL_REQUESTS_REPOS = "need_sync_github_pull_requests_repos"
NEED_SYNC_GITHUB_COMMITS_REPOS = "need_sync_github_commits_repos"
DURATION_OF_SYNC_GITHUB_PROFILES = "duration_of_sync_github_profiles"
SYNC_PROFILES_TASK_NUM = "sync_profiles_task_num"

NEED_INIT_GITS = "need_init_gits"
NEED_INIT_GITS_MODIFY_FILES = "need_init_gits_modify_files"
GITS_PROXY_CONFIG = "gits_proxy_config"

CLICKHOUSE_DRIVER_INFO = "clickhouse_conn_data"
SYNC_FROM_CLICKHOUSE_DRIVER_INFO = "sync_from_clickhouse_conn_data"
CK_TABLE_MAP_FROM_OS_INDEX = "ck_table_map_from_os_index"
CK_TABLE_SYNC_MAP_FROM_OS_INDEX = "ck_table_sync_map_from_os_index"
CK_CREATE_TABLE_COLS_DATATYPE_TPLT = "ck_create_table_cols_datatype_tplt"
CK_ALTER_TABLE_COLS_DATATYPE_TPLT = "ck_alter_table_cols_datatype_tplt"
CK_TABLE_DEFAULT_VAL_TPLT = "ck_table_default_val_tplt"

MYSQL_CONN_INFO = "mysql_conn_info"
MYSQL_CREATE_TABLE_DDL = 'mysql_create_table_ddl'
REDIS_CLIENT_DATA = "redis_client_data"

LOCATIONGEO_TOKEN = "LocationGeo_token"

MAIL_LISTS = "mail_lists"

PROXY_CONFS = "proxy_confs"
REPO_LIST = "repo_list"
SYNC_REPO_LIST = "sync_repo_list"
MAILLIST_REPO = "maillist_repo"

# Variables for sync
# Interval to sync clickhouse data from other environments
CLICKHOUSE_SYNC_INTERVAL = "clickhouse_sync_interval"
# Clickhouse sync combination type
# Enum:
# union: union of local and remote owner repos
# intersection: intersection of local and remote owner repos
# only_local: only local owner repos
# only_remote: only remote owner repos
# diff_local: local owner repos - remote owner repos
# diff_remote: remote owner repos - local owner repos
CLICKHOUSE_SYNC_COMBINATION_TYPE = "clickhouse_sync_combination_type"

# Interval to sync gits, github related data for all owner/repos
# in opensearch and then transfer the sync-ed part to clickhouse
DAILY_SYNC_INTERVAL = "daily_sync_interval"
DAILY_GITS_SYNC_INTERVAL = "daily_gits_sync_interval"
DAILY_GITHUB_COMMITS_SYNC_INTERVAL = "daily_github_commits_sync_interval"
DAILY_GITHUB_PRS_SYNC_INTERVAL = "daily_github_prs_sync_interval"
DAILY_GITHUB_ISSUES_SYNC_INTERVAL = "daily_github_issues_sync_interval"

ROUTINELY_UPDATE_METRICS_INTERVAL = "routinely_update_metrics_interval"
ROUTINELY_UPDATE_INFLUENCE_METRICS_INTERVAL = "routinely_update_influence_metrics_interval"

ROUTINELY_UPDATE_INFLUENCE_METRICS_INCLUDES = "routinely_update_influence_metrics_includes"

NEED_INIT_DISCOURSE = 'need_init_discourse'
NEED_INIT_DISCOURSE_CLICKHOUSE = 'need_init_discourse_clickhouse'

ZULIP_API_KEYS = "zulip_api_keys"
NEED_INIT_ZULIP = "need_init_zulip"

REPO_CLONE_DIR = "/tmp/gits"
