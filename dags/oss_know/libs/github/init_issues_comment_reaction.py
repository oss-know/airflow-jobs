import random
import time

import requests
from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers

from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX
from oss_know.libs.exceptions import GithubResourceNotFoundError
from oss_know.libs.util.base import concurrent_threads
from oss_know.libs.util.github_api import GithubAPI, GithubException
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


def init_github_issues_comment_reaction(opensearch_conn_info, owner, repo, token_proxy_accommodator, since=None):
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        request_timeout=120,
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    # 根据指定的owner/repo,获取现在所有的issues，并根据所有issues遍历相关的comments
    issues_comment_id = OpenSearchHelpers.scan(opensearch_client,
                                               index="github_issues_comments",
                                               query={
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
                                                               }
                                                           ],
                                                           "must_not": [
                                                               {
                                                                   "term": {
                                                                       "raw_data.reactions.total_count": {
                                                                           "value": 0
                                                                       }
                                                                   }
                                                               }
                                                           ]
                                                       }
                                                   },
                                                   "_source": ["search_key.owner", "search_key.repo",
                                                               "search_key.number",
                                                               "raw_data.id"]
                                               },
                                               request_timeout=120,
                                               size=5000,
                                               )
    need_init_comments_reactions_all_issues = []
    for comment_item in issues_comment_id:
        need_init_comments_reactions_all_issues.append(comment_item)

    # 提取需要同步的所有issues

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo 指定的issues comment记录的所有数据
    del_result = opensearch_client.delete_by_query(index="github_issues_comments_reactions",
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
                                                       },
                                                   },
                                                   request_timeout=120,
                                                   )

    logger.info(f"DELETE github issues comment reaction result:{del_result}")

    get_comment_reaction_tasks = list()
    get_comment_reaction_results = list()
    get_comment_reaction_fails_results = list()

    for idx, comment_item in enumerate(need_init_comments_reactions_all_issues):
        number = comment_item["_source"]["search_key"]["number"]
        comment_id = comment_item["_source"]["raw_data"]["id"]

        # 创建并发任务
        ct = concurrent_threads(do_get_github_comments_reaction,
                                args=(opensearch_client, token_proxy_accommodator, owner, repo, number, comment_id))
        get_comment_reaction_tasks.append(ct)
        ct.start()
        # 执行并发任务并获取结果
        if idx % 10 == 0:
            for tt in get_comment_reaction_tasks:
                tt.join()
                if tt.getResult()[0] != 200:
                    logger.info(f"get_timeline_fails_results:{tt},{tt.args}")
                    get_comment_reaction_fails_results.append(tt.getResult())

                get_comment_reaction_results.append(tt.getResult())
        if len(get_comment_reaction_fails_results) != 0:
            raise GithubException('github请求失败！', get_comment_reaction_fails_results)


# 在concurrent_threads中执行并发具体的业务方法
def do_get_github_comments_reaction(opensearch_client, token_proxy_accommodator, owner, repo, number, comment_id):
    req_session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()

    for page in range(1, 10000):
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))

        try:
            req = github_api.get_github_issues_comments_reactions(http_session=req_session,
                                                                  token_proxy_accommodator=token_proxy_accommodator,
                                                                  owner=owner,
                                                                  repo=repo,
                                                                  comment_id=comment_id,
                                                                  page=page)

            one_page_github_issues_comments_reaction = req.json()

        except GithubResourceNotFoundError as e:
            logger.error(
                f"fail init github timeline, {owner}/{repo}, issues_number:{number}, now_page:{page}, Target timeline info does not exist: {e}, end")
            # return 403, e

        logger.debug(f"one_page_github_issues_comments:{one_page_github_issues_comments_reaction}")

        if (one_page_github_issues_comments_reaction is not None) and len(
                one_page_github_issues_comments_reaction) == 0:
            logger.info(
                f"success init github comments reaction, {owner}/{repo}, issues_number:{number},comment id:{comment_id}, page_count:{page}, end")
            return 200, f"success init github comments reaction, {owner}/{repo}, issues_number:{number},comment id:{comment_id}, page_count:{page}, end"

            # logger.info(f"init sync github issues end to break:{owner}/{repo}/#{number} page_index:{page}")
            # break

        opensearch_api.bulk_github_issues_comments_reaction(opensearch_client=opensearch_client,
                                                            issues_comments_reactions=one_page_github_issues_comments_reaction,
                                                            owner=owner, repo=repo, number=number,
                                                            comment_id=comment_id)

        logger.info(f"success get github comments, {owner}/{repo}, issues_number:{number}, page_index:{page}, end")
