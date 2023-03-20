GITHUB_TOKENS = "github_tokens"
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

POSTGRES_CONN_INFO = "postgres_conn_info"

NEED_INIT_GITHUB_COMMITS_REPOS = "need_init_github_commits_repos"
NEED_INIT_GITHUB_ISSUES_REPOS = "need_init_github_issues_repos"
NEED_INIT_GITHUB_PULL_REQUESTS_REPOS = "need_init_github_pull_requests_repos"
NEED_INIT_GITHUB_ISSUES_TIMELINE_REPOS = "need_init_github_issues_timeline_repos"
NEED_INIT_GITHUB_ISSUES_COMMENTS_REPOS = "need_init_github_issues_comments_repos"
NEED_INIT_GITHUB_PROFILES_REPOS = "need_init_github_profiles_repos"
NEED_SYNC_GITHUB_PROFILES_REPOS = "need_sync_github_profiles_repos"
NEED_SYNC_GITHUB_ISSUES_REPOS = "need_sync_github_issues_repos"
NEED_SYNC_GITHUB_PULL_REQUESTS_REPOS = "need_sync_github_pull_requests_repos"
NEED_SYNC_GITHUB_COMMITS_REPOS = "need_sync_github_commits_repos"
DURATION_OF_SYNC_GITHUB_PROFILES = "duration_of_sync_github_profiles"
SYNC_PROFILES_TASK_NUM = "sync_profiles_task_num"

NEED_INIT_GITS = "need_init_gits"
GITS_PROXY_CONFIG = "gits_proxy_config"

CLICKHOUSE_DRIVER_INFO = "clickhouse_conn_data"
SYNC_FROM_CLICKHOUSE_DRIVER_INFO = "sync_from_clickhouse_conn_data"
CK_TABLE_MAP_FROM_OS_INDEX = "ck_table_map_from_os_index"
CK_TABLE_SYNC_MAP_FROM_OS_INDEX = "ck_table_sync_map_from_os_index"
CK_CREATE_TABLE_COLS_DATATYPE_TPLT = "ck_create_table_cols_datatype_tplt"
CK_ALTER_TABLE_COLS_DATATYPE_TPLT = "ck_alter_table_cols_datatype_tplt"
CK_TABLE_DEFAULT_VAL_TPLT = "ck_table_default_val_tplt"

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
DATA_SYNC_INTERVAL = "data_sync_interval"
NEED_INIT_DISCOURSE = 'need_init_discourse'
NEED_INIT_DISCOURSE_CLICKHOUSE = 'need_init_discourse_clickhouse'
