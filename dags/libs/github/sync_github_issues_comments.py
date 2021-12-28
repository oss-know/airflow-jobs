import datetime
import random

import requests
import time
import itertools
import copy

from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers

from .init_issues import get_github_issues, bulk_github_issues, sync_github_issues_check_update_info
from ..base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA, OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS
from ..util.base import github_headers, do_get_result
from ..util.log import logger


class SyncGithubIssuesCommentsException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_issues_comments(github_tokens, opensearch_conn_info, owner, repo, issues_numbers):
    logger.info("start sync_github_issues_comments()")
    github_tokens_iter = itertools.cycle(github_tokens)

    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    # has_issues_comments_check = opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
    #                                             body={
    #                                                 "size": 1,
    #                                                 "track_total_hits": True,
    #                                                 "query": {
    #                                                     "bool": {
    #                                                         "must": [
    #                                                             {
    #                                                                 "term": {
    #                                                                     "search_key.type.keyword": {
    #                                                                         "value": "github_issues"
    #                                                                     }
    #                                                                 }
    #                                                             },
    #                                                             {
    #                                                                 "term": {
    #                                                                     "search_key.owner.keyword": {
    #                                                                         "value": owner
    #                                                                     }
    #                                                                 }
    #                                                             },
    #                                                             {
    #                                                                 "term": {
    #                                                                     "search_key.repo.keyword": {
    #                                                                         "value": repo
    #                                                                     }
    #                                                                 }
    #                                                             }
    #                                                         ]
    #                                                     }
    #                                                 },
    #                                                 "sort": [
    #                                                     {
    #                                                         "search_key.update_timestamp": {
    #                                                             "order": "desc"
    #                                                         }
    #                                                     }
    #                                                 ]
    #                                             }
    #                                             )
    # if len(has_issues_comments_check["hits"]["hits"]) == 0:
    #     raise SyncGithubIssuesCommentsException("没有得到上次github issues 同步的时间")
    # github_issues_check = has_issues_comments_check["hits"]["hits"][0]["_source"]["github"]["issues"]
    #
    # # 生成本次同步的时间范围：同步到今天的 00:00:00
    # since = datetime.datetime.fromtimestamp(github_issues_check["sync_timestamp"]).strftime('%Y-%m-%dT00:00:00Z')
    # logger.info(f'sync github issues comments since：{since}')

    session = requests.Session()
    for issues_number in issues_numbers:
        del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS,
                                                       body={
                                                           "track_total_hits": True,
                                                           "query": {
                                                               "bool": {
                                                                   "must": [
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
                                                                       },
                                                                       {
                                                                           "term": {
                                                                               "search_key.number": {
                                                                                   "value": issues_number
                                                                               }
                                                                           }
                                                                       }
                                                                   ]
                                                               }
                                                           }
                                                       })
        logger.info(f"DELETE github issues {issues_number} comments result:{del_result}")

        for page in range(1, 10000):
            # Token sleep
            time.sleep(random.uniform(0.1, 0.5))

            req = get_github_issues_comments(session, github_tokens_iter, owner, repo, issues_number, page)
            one_page_github_issues_comments = req.json()

            if (one_page_github_issues_comments is not None) and len(one_page_github_issues_comments) == 0:
                logger.info(f"sync github issues comments end to break:{owner}/{repo} page_index:{page}")
                break

            bulk_github_issues_comments(one_page_github_issues_comments, opensearch_client, owner, repo, issues_number)

            logger.info(f"success get github issues comments page:{owner}/{repo} page_index:{page}")

    # 建立 sync 标志
    sync_github_issues_check_update_info(opensearch_client, owner, repo)


def get_github_issues_comments(req_session, github_tokens_iter, owner, repo, number, page):
    url = "https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments".format(
        owner=owner, repo=repo, number=number)
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {'per_page': 100, 'page': page}
    res = do_get_result(req_session, url, headers, params)
    return res


def bulk_github_issues_comments(now_github_issues_comments, opensearch_client, owner, repo, number):
    bulk_all_github_issues_comments = []

    template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS,
                "_source": {"search_key": {"owner": owner, "repo": repo, "number": number},
                            "raw_data": None}}
    commit_comment_item = copy.deepcopy(template)
    commit_comment_item["_source"]["raw_data"] = now_github_issues_comments
    bulk_all_github_issues_comments.append(commit_comment_item)
    logger.info(f"add init sync github issues comments number:{number}")

    success, failed = OpenSearchHelpers.bulk(client=opensearch_client, actions=bulk_all_github_issues_comments)
    logger.info(
        f"now page:{len(bulk_all_github_issues_comments)} sync github issues comments success:{success} & failed:{failed}")
