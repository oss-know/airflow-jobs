import random
import time

import requests
from opensearchpy import OpenSearch
from opensearchpy import helpers as opensearch_helpers

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES
from oss_know.libs.exceptions import GithubResourceNotFoundError
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def init_sync_github_issues_timeline(opensearch_conn_info, owner, repo, token_proxy_accommodator, since=None):
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    # 根据指定的 owner/repo , 获取现在所有的 issues，并根据所有 issues 便利相关的 comments
    scan_results = opensearch_helpers.scan(opensearch_client,
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
    need_init_sync_all_results = []
    for now_item in scan_results:
        need_init_sync_all_results.append(now_item)

    # 不要在dag or task里面 创建index 会有并发异常！！！
    # if not opensearch_client.indices.exists("github_issues"):
    #     opensearch_client.indices.create("github_issues")

    # todo 这里要将注释释放掉，本次注释应对暴力获取没爬完的数据
    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo 指定的 issues_timeline 记录的所有数据
    # del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
    #                                                body={
    #                                                    "query": {
    #                                                        "bool": {"must": [
    #                                                            {"term": {
    #                                                                "search_key.owner.keyword": {
    #                                                                    "value": owner
    #                                                                }
    #                                                            }},
    #                                                            {"term": {
    #                                                                "search_key.repo.keyword": {
    #                                                                    "value": repo
    #                                                                }
    #                                                            }}
    #                                                        ]}
    #                                                    }
    #                                                })

    # logger.info(f"DELETE github issues_timeline result:", del_result)

    req_session = requests.Session()
    opensearch_api = OpensearchAPI()
    github_api = GithubAPI()

    for now_item in need_init_sync_all_results:
        number = now_item["_source"]["raw_data"]["number"]
        # todo 这里要记得删掉应对获取未被获取的数据
        result = opensearch_client.search(index='github_issues_timeline', body={
            "query": {
                "bool": {
                    "must": [
                        {"term": {
                            "search_key.owner.keyword": {
                                "value": owner
                            }
                        }},
                        {"term": {
                            "search_key.repo.keyword": {
                                "value": repo
                            }
                        }},
                        {"term": {
                            "search_key.number": {
                                "value": number
                            }
                        }}
                    ]
                }
            }
        })
        # 如果在timeline里有这个issue的数据就跳过这个issue
        if result['hits']['hits']:
            logger.info(f'这个issue {number} 已经存在需要跳过')
            continue

        for page in range(1, 10000):
            time.sleep(random.uniform(0.05, 0.1)
            try:
                req = github_api.get_github_issues_timeline(req_session, token_proxy_accommodator, owner, repo, number,
                                                            page)
                one_page_github_issues_timeline = req.json()
            except GithubResourceNotFoundError as e:
                logger.error(f'Target timeline info does not exist: {e}')
                break

            if (one_page_github_issues_timeline is not None) and len(
                    one_page_github_issues_timeline) == 0:
                logger.info(f"init sync github issues end to break:{owner}/{repo} page_index:{page}")
                break

            opensearch_api.bulk_github_issues_timeline(opensearch_client=opensearch_client,
                                                       issues_timelines=one_page_github_issues_timeline,
                                                       owner=owner, repo=repo, number=number)

            logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")

# def get_github_issues_timeline(req_session, github_tokens_iter, owner, repo, number, page,
#                                since):
#     url = "https://api.github.com/repos/{owner}/{repo}/issues/{number}/timeline".format(
#         owner=owner, repo=repo, number=number)
#     headers = copy.deepcopy(github_headers)
#     headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
#     params = {'per_page': 100, 'page': page, 'since': since}
#     res = do_get_result(req_session, url, headers, params)
#     return res
#
#
# def bulk_github_pull_issues_timeline(now_github_issues_timeline, opensearch_client, owner, repo, number):
#     bulk_all_datas = []
#
#     for val in now_github_issues_timeline:
#         template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
#                     "_source": {"search_key": {"owner": owner, "repo": repo, "number": number},
#                                 "raw_data": None}}
#         append_item = copy.deepcopy(template)
#         append_item["_source"]["raw_data"] = val
#         bulk_all_datas.append(append_item)
#         logger.info(f"add init sync github issues_timeline number:{number}")
#
#     success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_datas)
#     logger.info(f"now page:{len(bulk_all_datas)} sync github issues_timeline success:{success} & failed:{failed}")
