import datetime
import random
import requests
import time
import itertools

from opensearchpy import OpenSearch

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA, OPENSEARCH_INDEX_GITHUB_ISSUES
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX


class SyncGithubIssuesException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


def sync_github_issues(opensearch_conn_info, owner, repo, token_proxy_accommodator):
    logger.info(f"Start sync github issues of {owner}/{repo}")
    now_time = datetime.datetime.now()
    opensearch_client = OpenSearch(hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
                                   http_compress=True,
                                   http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
                                   use_ssl=True, verify_certs=False, ssl_assert_hostname=False, ssl_show_warn=False)
    opensearch_api = OpensearchAPI()

    since = None
    issue_checkpoint = opensearch_api.get_checkpoint(opensearch_client,
                                                     OPENSEARCH_INDEX_GITHUB_ISSUES, owner, repo)
    if not issue_checkpoint["hits"]["hits"]:
        last_issue_date_str = get_latest_issue_date_str(opensearch_client, owner, repo)
        if not last_issue_date_str:
            raise SyncGithubIssuesException(f"没有得到上次github issues {owner}/{repo} 同步的时间")
        else:
            since = datetime.datetime.strptime(last_issue_date_str, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%dT00:00:00Z')
    else:
        github_issues_check = issue_checkpoint["hits"]["hits"][0]["_source"]["github"]["issues"]
        since = datetime.datetime.fromtimestamp(github_issues_check["sync_timestamp"]).strftime('%Y-%m-%dT00:00:00Z')
    logger.info(f'Sync github issues {owner}/{repo} since：{since}')

    issues_numbers = []
    pr_numbers = []
    session = requests.Session()
    github_api = GithubAPI()
    for page in range(1, 100000):
        # Token sleep
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))

        req = github_api.get_github_issues(http_session=session, token_proxy_accommodator=token_proxy_accommodator,
                                           owner=owner, repo=repo, page=page, since=since)

        one_page_github_issues = req.json()

        # 提取 issues number，返回给后续task 获取 issues comments & issues timeline
        for now_github_issues in one_page_github_issues:
            issues_numbers.append(now_github_issues["number"])
            if now_github_issues["node_id"].startswith('PR'):
                pr_numbers.append(now_github_issues["number"])

        if (one_page_github_issues is not None) and len(one_page_github_issues) == 0:
            logger.info(f"sync github issues end to break:{owner}/{repo} page_index:{page}")
            break

        opensearch_api.bulk_github_issues(opensearch_client=opensearch_client, github_issues=one_page_github_issues,
                                          owner=owner, repo=repo, if_sync=1)
        logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")

    # 建立 sync 标志
    opensearch_api.set_sync_github_issues_check(opensearch_client=opensearch_client, owner=owner, repo=repo,
                                                now_time=now_time)

    logger.info(f"issues_list:{issues_numbers}")

    # issues number，返回给后续task 获取 issues comments & issues timeline
    return issues_numbers, pr_numbers


def get_latest_issue_date_str(opensearch_client, owner, repo):
    last_issue_info = opensearch_client.search(index=OPENSEARCH_INDEX_GITHUB_ISSUES, body={
        "query": {
            "bool": {
                "must": [
                    {"term": {"search_key.owner.keyword": {"value": owner}}},
                    {"term": {"search_key.repo.keyword": {"value": repo}}}]
            }
        },
        "size": 1, "_source": "raw_data.created_at",
        "sort": [{"raw_data.created_at": {"order": "desc"}}]
    })
    if not last_issue_info["hits"]["hits"]:
        return None

    return last_issue_info['hits']['hits'][0]['_source']['raw_data']['created_at']
