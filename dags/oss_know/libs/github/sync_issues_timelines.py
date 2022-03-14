import random

import requests
import time
import itertools

from opensearchpy import OpenSearch

# from .init_issues_timeline import get_github_issues_timeline
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX


def sync_github_issues_timelines(github_tokens, opensearch_conn_info, owner, repo, issues_numbers):
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
    http_session = requests.Session()
    opensearch_api = OpensearchAPI()
    github_api = GithubAPI()

    for issues_number in issues_numbers:

        # 同步前删除原来存在的issues_numbers对应timeline
        del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
                                                       body={
                                                           "size": 10000,
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
        logger.info(f"DELETE github issues {issues_number} timeline result:{del_result}")

        for page in range(1, 10000):
            # Token sleep
            time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))
            req = github_api.get_github_issues_timeline(http_session=http_session,
                                                        github_tokens_iter=github_tokens_iter,
                                                        owner=owner, repo=repo, number=issues_number, page=page)
            one_page_github_issues_timeline = req.json()

            if (one_page_github_issues_timeline is not None) and len(
                    one_page_github_issues_timeline) == 0:
                logger.info(
                    f"sync github issues timeline end to break:{owner}/{repo}/{issues_number} page_index:{page}")
                break

            opensearch_api.bulk_github_issues_timeline(opensearch_client=opensearch_client,
                                                       issues_timelines=one_page_github_issues_timeline,
                                                       owner=owner, repo=repo, number=issues_number)

            logger.info(f"success get github issues timeline page:{owner}/{repo}/{issues_number} page_index:{page}")
