import random

import requests
import time
import itertools

from opensearchpy import OpenSearch

from ..base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS
from ..util.github_api import GithubAPI
from ..util.opensearch_api import OpensearchAPI
from ..util.log import logger


def init_sync_github_pull_requests(github_tokens, opensearch_conn_info, owner, repo, since=None):
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

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo的所有数据
    del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
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
    logger.info(f"DELETE github pull_requests result:{del_result}")

    # 获取指定owner/page 的 pull_requests
    session = requests.Session()
    opensearch_api = OpensearchAPI()
    github_api = GithubAPI()

    for page in range(1, 10000):
        # Token sleep
        time.sleep(random.uniform(0.1, 0.2))

        req = github_api.get_github_pull_requests(http_session=session, github_tokens_iter=github_tokens_iter,
                                                  owner=owner, page=page, repo=repo, since=since)

        one_page_github_pull_requests = req.json()

        if (one_page_github_pull_requests is not None) and len(one_page_github_pull_requests) == 0:
            logger.info(f"init sync github pull_requests end to break:{owner}/{repo} page_index:{page}")
            break

        opensearch_api.bulk_github_pull_requests(opensearch_client=opensearch_client,
                                                 github_pull_requests=one_page_github_pull_requests,
                                                 owner=owner, repo=repo)

        logger.info(f"success get github pull_requests page:{owner}/{repo} page_index:{page}")
