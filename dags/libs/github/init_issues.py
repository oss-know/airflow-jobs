import random
import datetime
import dateutil.parser
import requests
import time
import itertools
import copy

from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers

from ..base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA, OPENSEARCH_INDEX_GITHUB_ISSUES
from ..util.base import github_headers, do_get_result
from ..util.log import logger


def init_github_issues(github_tokens, opensearch_conn_infos, owner, repo, since=None):
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

    session = requests.Session()
    for page in range(1, 10000):
        # Token sleep
        time.sleep(random.uniform(0.1, 0.5))

        req = get_github_issues(session, github_tokens_iter, owner, repo, page, since)
        one_page_github_issues = req.json()

        if (one_page_github_issues is not None) and len(one_page_github_issues) == 0:
            logger.info(f"init sync github issues end to break:{owner}/{repo} page_index:{page}")
            break

        bulk_github_issues(one_page_github_issues, opensearch_client, owner, repo)

        logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")

    # 建立 sync 标志
    sync_github_issues_check_update_info(opensearch_client, owner, repo)


def get_github_issues(session, github_tokens_iter, owner, repo, page, since):
    url = "https://api.github.com/repos/{owner}/{repo}/issues".format(
        owner=owner, repo=repo)
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {'state': 'all', 'per_page': 100, 'page': page, 'since': since}
    res = do_get_result(session, url, headers, params)

    logger.info(f"url:{url}, \n headers:{headers}, \n params：{params}")

    return res


def bulk_github_issues(now_github_issues, opensearch_client, owner, repo):
    bulk_all_github_issues = []

    for now_issue in now_github_issues:
        del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES,
                                                       body={
                                                           "track_total_hits": True,
                                                           "query": {
                                                               "bool": {
                                                                   "must": [
                                                                       {
                                                                           "term": {
                                                                               "raw_data.number": {
                                                                                   "value": now_issue["number"]
                                                                               }
                                                                           }
                                                                       },
                                                                       {
                                                                           "term": {
                                                                               "search_key.owner.keyword": {
                                                                                   "value": owner
                                                                               }
                                                                           }
                                                                       },
                                                                       {
                                                                           "term": {
                                                                               "search_key.repo.keyword": {
                                                                                   "value": repo
                                                                               }
                                                                           }
                                                                       }
                                                                   ]
                                                               }
                                                           }
                                                       })
        logger.info(f"DELETE github issues result:{del_result}")

        template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES,
                    "_source": {"search_key": {"owner": owner, "repo": repo},
                                "raw_data": None}}
        commit_item = copy.deepcopy(template)
        commit_item["_source"]["raw_data"] = now_issue
        bulk_all_github_issues.append(commit_item)
        logger.info(f"add sync github issues number:{now_issue['number']}")

    success, failed = OpenSearchHelpers.bulk(client=opensearch_client, actions=bulk_all_github_issues)
    logger.info(f"now page:{len(bulk_all_github_issues)} sync github issues success:{success} & failed:{failed}")

    return success, failed


# 建立 owner/repo github issues 更新基准
def sync_github_issues_check_update_info(opensearch_client,
                                         owner,
                                         repo):
    now_time = datetime.datetime.now()
    check_update_info = {
        "search_key": {
            "type": "github_issues",
            "update_time": now_time.strftime('%Y-%m-%dT00:00:00Z'),
            "update_timestamp": now_time.timestamp(),
            "owner": owner,
            "repo": repo
        },
        "github": {
            "type": "github_issues",
            "owner": owner,
            "repo": repo,
            "issues": {
                "owner": owner,
                "repo": repo,
                "sync_datetime": now_time.strftime('%Y-%m-%dT00:00:00Z'),
                "sync_timestamp": now_time.timestamp()
            }
        }
    }

    opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                            body=check_update_info,
                            refresh=True)
