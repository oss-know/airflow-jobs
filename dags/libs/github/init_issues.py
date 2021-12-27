import requests
import time
import itertools
import copy

from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers

from ..util.base import github_headers, do_get_result
from ..util.log import logger

OPENSEARCH_INDEX_GITHUB_ISSUES = "github_issues"


def init_sync_github_issues(github_tokens, opensearch_conn_infos, owner, repo, since=None):
    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    # 不要在dag or task里面 创建index 会有并发异常！！！
    # if not opensearch_client.indices.exists("github_issues"):
    #     opensearch_client.indices.create("github_issues")

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo的所有数据
    del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES,
                                                   body={
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
                                                   })
    logger.info("DELETE github issues result:{del_result}")

    session = requests.Session()
    for page in range(1, 10000):
        # Token sleep
        time.sleep(1)

        req = get_github_issues(session, github_tokens_iter, opensearch_conn_infos, owner, page, repo, since)
        one_page_github_issues = req.json()

        if (one_page_github_issues is not None) and len(one_page_github_issues) == 0:
            logger.info(f"init sync github issues end to break:{owner}/{repo} page_index:{page}")
            break

        bulk_github_issues(one_page_github_issues, opensearch_client, owner, repo)

        logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")


def get_github_issues(session, github_tokens_iter, opensearch_conn_infos, owner, page, repo, since):
    url = "https://api.github.com/repos/{owner}/{repo}/issues".format(
        owner=owner, repo=repo)
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {'state': 'all', 'per_page': 100, 'page': page, 'since': since}
    res = do_get_result(session, url, headers, params)

    return res


def bulk_github_issues(now_github_issues, opensearch_client, owner, repo):
    bulk_all_github_issues = []

    for now_issue in now_github_issues:
        template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES,
                    "_source": {"search_key": {"owner": owner, "repo": repo},
                                "raw_data": None}}
        commit_item = copy.deepcopy(template)
        commit_item["_source"]["raw_data"] = now_issue
        bulk_all_github_issues.append(commit_item)
        logger.info(f"add init sync github issues number:{now_issue['number']}")

    success, failed = OpenSearchHelpers.bulk(client=opensearch_client, actions=bulk_all_github_issues)
    logger.info(f"now page:{len(bulk_all_github_issues)} sync github issues success:{success} & failed:{failed}")

    return success, failed
