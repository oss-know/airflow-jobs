import datetime
import random
import time

import requests
from opensearchpy import OpenSearch

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_COMMITS, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI


class SyncGithubCommitException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_commits_opensearch(opensearch_conn_info,
                                   owner,
                                   repo,
                                   token_proxy_accommodator):
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

    # 取得上次更新github commit 的时间节点
    since = None
    commit_checkpoint = opensearch_api.get_checkpoint(opensearch_client,
                                                      OPENSEARCH_INDEX_GITHUB_COMMITS, owner, repo)
    if len(commit_checkpoint["hits"]["hits"]) == 0:
        # raise SyncGithubCommitException("没有得到上次github commits 同步的时间")
        # Try to get the latest commit date(committed_date field) from existing github_commits index
        # And make it the latest checkpoint
        latest_commit_date_str = get_latest_commit_date_str(opensearch_client, owner, repo)
        if not latest_commit_date_str:
            raise SyncGithubCommitException("没有得到上次github commits 同步的时间")
        since = datetime.datetime.strptime(latest_commit_date_str, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%dT00:00:00Z')
    else:
        github_commits_check = commit_checkpoint["hits"]["hits"][0]["_source"]["github"]["commits"]
        since = datetime.datetime.fromtimestamp(github_commits_check["sync_until_timestamp"]).strftime(
            '%Y-%m-%dT00:00:00Z')

    # 生成本次同步的时间范围：同步到今天的 00:00:00
    until = datetime.datetime.now().strftime('%Y-%m-%dT00:00:00Z')
    logger.info(f'Sync github commits {owner}/{repo} since：{since}，sync until：{until}')

    session = requests.Session()
    github_api = GithubAPI()
    for page in range(1, 999999):
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))
        req = github_api.get_github_commits(http_session=session,
                                            token_proxy_accommodator=token_proxy_accommodator,
                                            owner=owner,
                                            repo=repo,
                                            page=page,
                                            since=since,
                                            until=until)
        now_github_commits = req.json()

        if (now_github_commits is not None) and len(now_github_commits) == 0:
            logger.info(f'get github commits end to break:: {owner}/{repo} page_index:{page}')
            break

        opensearch_api.bulk_github_commits(opensearch_client=opensearch_client,
                                           github_commits=now_github_commits,
                                           owner=owner, repo=repo, if_sync=1)

        logger.info(f"success get github commits :: {owner}/{repo} page_index:{page}")

    opensearch_api.set_sync_github_commits_check(opensearch_client, owner, repo, since, until)

    return "END::sync_github_commits"


def get_latest_commit_date_str(opensearch_client, owner, repo):
    latest_commit_info = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_COMMITS,
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
                                                              "raw_data.commit.committer.date": {
                                                                  "order": "desc"
                                                              }
                                                          }
                                                      ]
                                                  }
                                                  )
    if len(latest_commit_info["hits"]["hits"]) == 0:
        return None

    return latest_commit_info["hits"]["hits"][0]["_source"]["raw_data"]["commit"]["committer"]["date"]
