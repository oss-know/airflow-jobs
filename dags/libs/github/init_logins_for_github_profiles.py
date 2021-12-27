from . import init_profile_commen
from opensearchpy.helpers import scan as os_scan
from loguru import logger


# todo 抽象成类，设置全局属性
def load_github_logins_by_repo(opensearch_conn_infos, owner, repo):
    init_profile_logins = load_logins_by_github_commits(opensearch_conn_infos, owner, repo)
    init_profile_logins += load_logins_by_github_issues(opensearch_conn_infos, owner, repo)
    init_profile_logins += load_logins_by_github_issues_comments(opensearch_conn_infos, owner,
                                                                                           repo)
    init_profile_logins += load_logins_by_github_issues_timeline(opensearch_conn_infos, owner,
                                                                                           repo)
    init_profile_logins += load_logins_by_pull_requests(opensearch_conn_infos, owner, repo)
    return init_profile_logins


def load_logins_by_github_commits(opensearch_conn_infos, owner, repo):
    """Get GitHub users' logins from GitHub commits."""

    res = get_github_data_by_repo_owner_index_from_os(opensearch_conn_infos, owner, repo, index='github_commits')

    # 对github author 和 committer 去重
    all_commits_users_dict = {}
    all_commits_users = []

    for commit in res:
        raw_data = commit["_source"]["raw_data"]
        if (raw_data["author"] is not None) and ("author" in raw_data) and ("login" in raw_data["author"]):
            all_commits_users_dict[raw_data["author"]["login"]] = \
                raw_data["author"]["url"]
            all_commits_users.append(raw_data["author"]["login"])
        if (raw_data["committer"] is not None) and ("committer" in raw_data) and ("login" in raw_data["committer"]):
            all_commits_users_dict[raw_data["committer"]["login"]] = \
                raw_data["committer"]["url"]
            all_commits_users.append(raw_data["committer"]["login"])
    logger.info(load_logins_by_github_commits.__doc__)
    return all_commits_users


def load_logins_by_github_issues(opensearch_conn_infos, owner, repo):
    """Get GitHub users' logins from GitHub issues ."""

    res = get_github_data_by_repo_owner_index_from_os(opensearch_conn_infos, owner, repo, index='github_issues')

    all_issues_users = []
    if res is None:
        logger.info(f"There's no github issues in {repo}")
    else:

        for issue in res:
            raw_data = issue["_source"]["raw_data"]["user"]["login"]
            all_issues_users.append(raw_data)
        logger.info(load_logins_by_github_issues.__doc__)
    return all_issues_users


def load_logins_by_github_issues_comments(opensearch_conn_infos, owner, repo):
    """Get GitHub user's login from GitHub issues comments."""

    res = get_github_data_by_repo_owner_index_from_os(opensearch_conn_infos, owner, repo,
                                                      index='github_issues_comments')

    all_issues_comments_users = []
    if res is None:
        logger.info(f"There's no github issues' comments in {repo}")
    else:
        for issue_comment in res:
            issue_comment_user_login = issue_comment["_source"]["raw_data"]["user"]["login"]
            all_issues_comments_users.append(issue_comment_user_login)

        logger.info(load_logins_by_github_issues_comments.__doc__)
    return all_issues_comments_users


def load_logins_by_github_issues_timeline(opensearch_conn_infos, owner, repo):
    """Get GitHub users' logins from GitHub issues timeline ."""

    res = get_github_data_by_repo_owner_index_from_os(opensearch_conn_infos, owner, repo,
                                                      index='github_issues_timeline')

    all_issues_timeline_users = []
    if res is None:
        logger.info(f"There's no github issues' timeline in {repo}")
    else:
        for issue_timeline in res:
            issue_timeline_raw_data = issue_timeline["_source"]["raw_data"]
            if issue_timeline_raw_data["event"] != "committed":
                if "user" in issue_timeline_raw_data:
                    issue_timeline_user_login = issue_timeline_raw_data["user"]["login"]
                    all_issues_timeline_users.append(issue_timeline_user_login)
                else:
                    issue_timeline_user_login = issue_timeline_raw_data["actor"]["login"]
                    all_issues_timeline_users.append(issue_timeline_user_login)

        logger.info(load_logins_by_github_issues_timeline.__doc__)
    return all_issues_timeline_users


def load_logins_by_pull_requests(opensearch_conn_infos, owner, repo):
    """Get GitHub users' logins from GitHub pull requests."""

    all_pull_requests_users = []
    res = get_github_data_by_repo_owner_index_from_os(opensearch_conn_infos, owner, repo, index='github_pull_requests')

    if res is None:
        logger.info(f"There's no github issues in {repo}")
    else:
        for pull_request in res:
            raw_data = pull_request["_source"]["raw_data"]["user"]["login"]
            all_pull_requests_users.append(raw_data)

        logger.info(load_logins_by_pull_requests.__doc__)
    return all_pull_requests_users


def get_github_data_by_repo_owner_index_from_os(opensearch_conn_infos, owner, repo, index):
    """Get GitHub users by repo and owner and index from opensearch."""

    opensearch_client = init_profile_commen.get_opensearch_client(opensearch_conn_infos)
    # 查询owner+repo所有github issues记录用来提取github issue的user
    res = os_scan(client=opensearch_client, index=index,
                  query={
                      "track_total_hits": True,
                      "query": {
                          "bool": {"must": [
                              {"term": {
                                  "search_key.owner.keyword": {
                                      "value": owner
                                  }
                              }},
                              {"term": {
                                  "search_key.repo.keyword": {
                                      "value": repo
                                  }
                              }}
                          ]}
                      },
                      "size": 10
                  }, doc_type='_doc', timeout='10m')
    logger.info(f'Get GitHub users by {repo} and {owner} and {index} from opensearch.')
    return res
