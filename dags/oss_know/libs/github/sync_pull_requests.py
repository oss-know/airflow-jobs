import datetime
import random
import time

import requests
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


class SyncGithubPullRequestsException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_pull_requests(opensearch_conn_info,
                              owner,
                              repo,
                              token_proxy_accommodator
                              ):
    logger.info("start Function to be renamed to sync_github_pull_requests")
    now_time = datetime.datetime.now()
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    opensearch_api = OpensearchAPI()

    since = None
    pr_checkpoint = opensearch_api.get_checkpoint(opensearch_client,
                                                  OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS, owner, repo)
    if len(pr_checkpoint["hits"]["hits"]) == 0:
        # Try to get the latest PR date(created_at field) from existing github_pull_requests index
        # And make it the latest checkpoint
        latest_pr_date = get_latest_pr_date_str(opensearch_client, owner, repo)
        if not latest_pr_date:
            raise SyncGithubPullRequestsException("没有得到上次github pull_requests 同步的时间")
        since = datetime.datetime.strptime(latest_pr_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%dT00:00:00Z')
    else:
        github_pull_requests_check = pr_checkpoint["hits"]["hits"][0]["_source"]["github"]["prs"]
        since = datetime.datetime.fromtimestamp(github_pull_requests_check["sync_timestamp"]).strftime(
            '%Y-%m-%dT00:00:00Z')

    # 生成本次同步的时间范围：同步到今天的 00:00:00
    logger.info(f'Sync github pull requests {owner}/{repo} since：{since}')

    pull_requests_numbers = []
    session = requests.Session()
    github_api = GithubAPI()
    for page in range(1, 100000):
        # Token sleep
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))

        req = github_api.get_github_pull_requests(http_session=session,
                                                  token_proxy_accommodator=token_proxy_accommodator,
                                                  owner=owner,
                                                  repo=repo,
                                                  page=page)

        one_page_github_pull_requests = req.json()
        all_new_prs = []
        # 将since时间之后的插入，遇到since时间之前的数据直接舍弃
        for now_github_pull_requests in one_page_github_pull_requests:
            if now_github_pull_requests['updated_at'] < since:
                if all_new_prs:
                    opensearch_api.sync_bulk_github_pull_requests(github_pull_requests=all_new_prs,
                                                                  opensearch_client=opensearch_client,
                                                                  owner=owner, repo=repo, if_sync=1)
                return pull_requests_numbers
            all_new_prs.append(now_github_pull_requests)
            pull_requests_numbers.append(now_github_pull_requests["number"])

        if (all_new_prs is not None) and len(all_new_prs) == 0:
            logger.info(f"sync github pull_requests end to break:{owner}/{repo} page_index:{page}")
            break

        opensearch_api.sync_bulk_github_pull_requests(github_pull_requests=all_new_prs,
                                                      opensearch_client=opensearch_client,
                                                      owner=owner, repo=repo, if_sync=1)
        logger.info(f"success get github pull_requests page:{owner}/{repo} page_index:{page}")

    # 建立 sync 标志
    opensearch_api.set_sync_github_pull_requests_check(opensearch_client=opensearch_client,
                                                       owner=owner, repo=repo, now_time=now_time)

    logger.info(f"pull_requests_list:{pull_requests_numbers}")

    # todo 返回 pull_requests number，获取 pull_requests 独有的更新，进行后续处理
    return pull_requests_numbers


def get_latest_pr_date_str(opensearch_client, owner, repo):
    latest_pr_info = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS,
                                              body={
                                                  "size": 1,
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
                                                          ]
                                                      }
                                                  },
                                                  "sort": [
                                                      {
                                                          "raw_data.created_at": {
                                                              "order": "desc"
                                                          }
                                                      }
                                                  ]
                                              }
                                              )
    if len(latest_pr_info["hits"]["hits"]) == 0:
        return None

    return latest_pr_info["hits"]["hits"][0]["_source"]["raw_data"]["created_at"]
