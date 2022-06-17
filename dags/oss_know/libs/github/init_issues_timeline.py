import random
import time
import requests
import datetime
import numpy as np
import json
from threading import Thread

from opensearchpy import OpenSearch
from opensearchpy import helpers as opensearch_helpers

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.exceptions import GithubResourceNotFoundError
from oss_know.libs.util.base import concurrent_threads
from oss_know.libs.util.github_api import GithubAPI, GithubException
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX


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
                                           }
                                           )
    need_init_all_results = []
    for now_item in scan_results:
        need_init_all_results.append(now_item)

    # 不要在dag or task里面 创建index 会有并发异常！！！
    # if not opensearch_client.indices.exists("github_issues"):
    #     opensearch_client.indices.create("github_issues")

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo 指定的 issues_timeline 记录的所有数据
    del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
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

    logger.info(f"DELETE github issues_timeline result:", del_result)

    get_timeline_tasks = list()
    get_timeline_results = list()
    get_timeline_fails_results = list()
    for idx, now_item in enumerate(need_init_all_results):
        number = now_item["_source"]["raw_data"]["number"]

        # 创建并发任务
        ct = concurrent_threads(do_get_github_timeline, args=(opensearch_client, token_proxy_accommodator, owner, repo, number))
        get_timeline_tasks.append(ct)
        ct.start()

        # 执行并发任务并获取结果
        if idx % 30 == 0:
            for tt in get_timeline_tasks:
                tt.join()
                if tt.getResult()[0] != 200:
                    logger.info(f"get_timeline_fails_results:{tt},{tt.args}")
                    get_timeline_fails_results.append(tt.getResult())

                get_timeline_results.append(tt.getResult())
        if len(get_timeline_fails_results) != 0:
            raise GithubException('github请求失败！', get_timeline_fails_results)



# 在concurrent_threads中执行并发具体的业务方法
def do_get_github_timeline(opensearch_client, token_proxy_accommodator, owner, repo, number):
    req_session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()

    for page in range(1, 10000):
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))

        try:
            req = github_api.get_github_issues_timeline(
                req_session, token_proxy_accommodator, owner, repo, number, page)
            one_page_github_issues_timeline = req.json()
        except GithubResourceNotFoundError as e:
            logger.error(f"fail init github timeline, {owner}/{repo}, issues_number:{number}, now_page:{page}, Target timeline info does not exist: {e}, end")
            # return 403, e

        if (one_page_github_issues_timeline is not None) and len(
                one_page_github_issues_timeline) == 0:
            logger.info(f"success init github timeline, {owner}/{repo}, issues_number:{number}, page_count:{page}, end")
            return 200, f"success init github timeline, {owner}/{repo}, issues_number:{number}, page_count:{page}, end"

        opensearch_api.bulk_github_issues_timeline(opensearch_client=opensearch_client,
                                                   issues_timelines=one_page_github_issues_timeline,
                                                   owner=owner, repo=repo, number=number)

        logger.info(f"success get github timeline, {owner}/{repo}, issues_number:{number}, page_index:{page}, end")
