import datetime
import random
import requests
import time
import itertools

from opensearchpy import OpenSearch

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


class SyncGithubPullRequestsException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_pull_requests(github_tokens, opensearch_conn_info, owner, repo):
    logger.info("start Function to be renamed to sync_github_pull_requests")
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

    has_pull_requests_check = opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA,
                                                       body={}
                                                       )
    if len(has_pull_requests_check["hits"]["hits"]) == 0:
        raise SyncGithubPullRequestsException("没有得到上次github pull_requests 同步的时间")
    github_pull_requests_check = has_pull_requests_check["hits"]["hits"][0]["_source"]["github"]["pull_requests"]

    # 生成本次同步的时间范围：同步到今天的 00:00:00
    since = datetime.datetime.fromtimestamp(github_pull_requests_check["sync_timestamp"]).strftime('%Y-%m-%dT00:00:00Z')
    logger.info(f'sync github pull_requests since：{since}')

    pull_requests_numbers = []
    session = requests.Session()
    opensearch_api = OpensearchAPI()
    github_api = GithubAPI()

    for page in range(1, 10000):
        # Token sleep
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))

        req = github_api.get_github_pull_requests(http_session=session,
                                                  github_tokens_iter=github_tokens_iter,
                                                  owner=owner, repo=repo, page=page, since=since)

        one_page_github_pull_requests = req.json()

        # todo 提取 pull_requests number，获取 pull_requests 独有的更新，进行后续处理
        for now_github_pull_requests in one_page_github_pull_requests:
            pull_requests_numbers.append(now_github_pull_requests["number"])

        if (one_page_github_pull_requests is not None) and len(one_page_github_pull_requests) == 0:
            logger.info(f"sync github pull_requests end to break:{owner}/{repo} page_index:{page}")
            break

        opensearch_api.bulk_github_pull_requests(github_pull_requests=one_page_github_pull_requests,
                                                 opensearch_client=opensearch_client,
                                                 owner=owner, repo=repo)
        logger.info(f"success get github pull_requests page:{owner}/{repo} page_index:{page}")

    # 建立 sync 标志
    opensearch_api.set_sync_github_pull_requests_check(opensearch_client=opensearch_client,
                                                owner=owner, repo=repo)

    logger.info(f"pull_requests_list:{pull_requests_numbers}")

    # todo 返回 pull_requests number，获取 pull_requests 独有的更新，进行后续处理
    return pull_requests_numbers
