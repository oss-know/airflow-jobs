import itertools
from loguru import logger
from opensearchpy.helpers import scan as os_scan
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_COMMITS, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.util.base import get_opensearch_client


def load_github_ids_by_repo(opensearch_conn_infos, owner, repo):
    """Get GitHub users' ids from GitHub assigned owner and repo."""
    opensearch_client = get_opensearch_client(opensearch_conn_infos)
    init_profile_ids = load_ids_from_issues_timeline(opensearch_client, owner,
                                                          repo)
    init_profile_ids += load_ids_from_commits(opensearch_client, owner, repo)
    return init_profile_ids


def load_ids_from_commits(opensearch_client, owner, repo):
    """Get GitHub users' ids from GitHub commits."""
    logger.debug(f'calling load_ids_by_github_commits for {owner}/{repo}')
    res = get_profiles_from_os(opensearch_client, owner, repo,
                                                      index=OPENSEARCH_INDEX_GITHUB_COMMITS)
    if not res:
        logger.info(f"There's no github commits in {repo}")
        return []
    # delete duplicated data of GitHub author and GitHub committer
    all_commits_ids = set()

    for commit in res:
        raw_data = commit["_source"]["raw_data"]
        if ("author" in raw_data) and raw_data["author"] and ("id" in raw_data["author"]):
            all_commits_ids.add(raw_data["author"]["id"])
        if ("committer" in raw_data) and raw_data["committer"] and  ("id" in raw_data["committer"]):
            all_commits_ids.add(raw_data["committer"]["id"])

    return list(all_commits_ids)


def load_ids_from_issues_timeline(opensearch_client, owner, repo):
    """Get GitHub users' ids from GitHub issues timeline ."""

    logger.debug(f'calling load_ids_by_github_issues_timeline for {owner}/{repo}')

    res = get_profiles_from_os(opensearch_client, owner, repo,
                                                      index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE)

    if not res:
        logger.info(f"There's no github issues' timeline in {repo}")
        return []

    all_issues_timeline_users = set()
    for issue_timeline in res:
        issue_timeline_raw_data = issue_timeline["_source"]["raw_data"]
        if issue_timeline_raw_data["event"] == "cross-referenced":
            all_issues_timeline_users.add(issue_timeline_raw_data["actor"]["id"])
            all_issues_timeline_users.add(issue_timeline_raw_data["source"]["issue"]["user"]["id"])
        elif issue_timeline_raw_data["event"] != "committed":
            for key in ["user", "actor", "assignee"]:
                if key in issue_timeline_raw_data:
                    all_issues_timeline_users.add(issue_timeline_raw_data[key]["id"])

    return list(all_issues_timeline_users)


def get_profiles_from_os(opensearch_client, owner, repo, index):
    """Get GitHub users by repo and owner and index from opensearch."""
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
                      }
                  }, doc_type='_doc', timeout='10m')
    logger.info(f'Get GitHub users by {repo} and {owner} and {index} from opensearch.')
    return res


def load_github_profiles(github_tokens, opensearch_conn_infos, github_users_ids):
    """Get GitHub profiles by ids."""
    logger.debug(f'calling load_github_profiles by {github_users_ids}')
    # get ids set;
    github_users_ids = list(set(github_users_ids))
    # put GitHub user profile into opensearch if it is not in opensearch
    github_tokens_iter = itertools.cycle(github_tokens)
    opensearch_api = OpensearchAPI()
    opensearch_api.put_profile_into_opensearch(github_ids=github_users_ids, github_tokens_iter=github_tokens_iter,
                                               opensearch_client=get_opensearch_client(opensearch_conn_infos))
