from . import init_profile_commen
import itertools
from opensearchpy.helpers import scan as os_scan
import time
from loguru import logger

OPEN_SEARCH_GITHUB_PROFILE_INDEX = "github_profile"


def load_github_profile_pull_requests(github_tokens, opensearch_conn_infos, owner, repo):
    """Get GitHub user's profile from GitHub pull requests and put it into opensearch if it is not in opensearch."""

    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = init_profile_commen.get_opensearch_client(opensearch_conn_infos)

    # 查询owner+repo所有github issues记录用来提取github issue的user
    res = os_scan(client=opensearch_client, index='github_pull_requests',
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
    if res is None:
        logger.info(f"There's no github issues in {repo}")
    else:
        all_pull_requests_users = set([])

        for pull_request in res:
            raw_data = pull_request["_source"]["raw_data"]["user"]["login"]
            all_pull_requests_users.add(raw_data)

        # # 获取github profile
        # init_profile_commen.put_profile_into_opensearch(opensearch_client, all_pull_requests_users,
        #                                                 OPEN_SEARCH_GITHUB_PROFILE_INDEX, github_tokens_iter,
        #                                                 opensearch_conn_infos)
        logger.info(load_github_profile_pull_requests.__doc__)
    return all_pull_requests_users
