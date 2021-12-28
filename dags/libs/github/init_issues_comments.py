import random

import requests
import time
import itertools
import copy

from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers

from .init_issues_timeline import OPENSEARCH_INDEX_GITHUB_ISSUES
from ..base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS
from ..util.base import github_headers, do_get_result
from ..util.log import logger




def get_github_issues_comments(req_session, github_tokens_iter, owner, repo, number, page,
                               since):
    url = "https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments".format(
        owner=owner, repo=repo, number=number)
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {'per_page': 100, 'page': page, 'since': since}
    res = do_get_result(req_session, url, headers, params)
    return res


def bulk_github_issues_comments(now_github_issues_comments, opensearch_client, owner, repo, number):
    bulk_all_github_issues_comments = []

    for now_issue_comments in now_github_issues_comments:
        template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS,
                    "_source": {"search_key": {"owner": owner, "repo": repo, "number": number},
                                "raw_data": None}}
        commit_comment_item = copy.deepcopy(template)
        commit_comment_item["_source"]["raw_data"] = now_issue_comments
        bulk_all_github_issues_comments.append(commit_comment_item)
        logger.info(f"add init sync github issues comments number:{number}")

    success, failed = OpenSearchHelpers.bulk(client=opensearch_client, actions=bulk_all_github_issues_comments)
    logger.info(
        f"now page:{len(bulk_all_github_issues_comments)} sync github issues comments success:{success} & failed:{failed}")


def init_sync_github_issues_comments(github_tokens, opensearch_conn_info, owner, repo, since=None):
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

    # 根据指定的owner/repo,获取现在所有的issues，并根据所有issues遍历相关的comments
    issues_results = OpenSearchHelpers.scan(opensearch_client,
                                            index=OPENSEARCH_INDEX_GITHUB_ISSUES,
                                            query={
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
                                            },
                                            doc_type="_doc"
                                            )
    need_init_sync_all_issues = []
    for issues_item in issues_results:
        need_init_sync_all_issues.append(issues_item)

    # 提取需要同步的所有issues

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo 指定的issues comment记录的所有数据
    del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS,
                                                   body={
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
    logger.info(f"DELETE github issues comment result:{del_result}")

    req_session = requests.Session()

    for issue_item in need_init_sync_all_issues:
        number = issue_item["_source"]["raw_data"]["number"]
        for page in range(1, 10000):
            time.sleep(random.uniform(0.1, 0.5))

            req = get_github_issues_comments(req_session, github_tokens_iter, owner, repo, number,
                                             page, since)
            one_page_github_issues_comments = req.json()

            if (one_page_github_issues_comments is not None) and len(one_page_github_issues_comments) == 0:
                logger.info(f"init sync github issues end to break:{owner}/{repo} page_index:{page}")
                break

            bulk_github_issues_comments(one_page_github_issues_comments, opensearch_client, owner, repo, number)

            logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")
