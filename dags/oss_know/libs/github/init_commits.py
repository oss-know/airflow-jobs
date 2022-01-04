import itertools
import random
import requests
import time

from opensearchpy import OpenSearch

from oss_know.libs.util.log import logger
from oss_know.libs.util.github_api import GithubAPI
from oss_know.libs.util.opensearch_api import OpensearchAPI


def init_github_commits(github_tokens,
                        opensearch_conn_info,
                        owner, repo, since=None, until=None):
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
    session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()
    for page in range(1, 9999):
        time.sleep(random.uniform(0.1, 0.2))
        # 获取一页 github commits
        req = github_api.get_github_commits(http_session=session, github_tokens_iter=github_tokens_iter, owner=owner,
                                            repo=repo, page=page, since=since, until=until)
        one_page_github_commits = req.json()

        # 没有更多的 github commits 则跳出循环
        if (one_page_github_commits is not None) and len(one_page_github_commits) == 0:
            logger.info(f'get github commits end to break:: {owner}/{repo} page_index:{page}')
            break

        # 向opensearch插入一页 github commits
        opensearch_api.bulk_github_commits(opensearch_client=opensearch_client,
                                           github_commits=one_page_github_commits,
                                           owner=owner, repo=repo)

        logger.info(f"success get github commits :: {owner}/{repo} page_index:{page}")

    opensearch_api.set_sync_github_commits_check(opensearch_client, owner, repo, since, until)

    return "END::init_github_commits"
