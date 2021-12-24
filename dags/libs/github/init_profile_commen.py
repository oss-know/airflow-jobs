import copy
import requests
from opensearchpy import OpenSearch
from ..util.base import github_headers, do_get_result, HttpGetException
from loguru import logger


def get_github_profile(github_tokens_iter, login_info, opensearch_conn_infos):
    """Get GitHub user's latest profile from GitHUb."""
    url = "https://api.github.com/users/{login_info}".format(
        login_info=login_info)

    github_headers.update({'Authorization': 'token %s' % next(github_tokens_iter),
                           'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                         'Chrome/96.0.4664.110 Safari/537.36'})

    # github_headers.update({'Authorization': 'token %s' % next(github_tokens_iter), 'user-agent': 'Mozilla/5.0 (X11;
    # Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'})
    headers = copy.deepcopy(github_headers)
    headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
    params = {}
    req = {}
    req_session = requests.Session()
    now_github_profile = {}

    try:
        req = do_get_result(req_session, url, headers, params)
        # if req.status_code != 200:
        #     raise Exception('获取github profile 失败！')
        now_github_profile = req.json()
    except HttpGetException as hge:
        print("遇到访问github api 错误！！！")
        print("opensearch_conn_info:", opensearch_conn_infos)
        print("url:", url)
        print("status_code:", req.status_code)
        print("headers:", headers)
        print("text:", req.text)
    except TypeError as e:
        print("捕获airflow抛出的TypeError:", e)
    # finally:
    #     req.close()
    logger.info(get_github_profile.__doc__)
    return now_github_profile


# 连接OpenSearch
def get_opensearch_client(opensearch_conn_infos):
    """Get opensearch client to connect to opensearch."""
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    logger.info(get_opensearch_client.__doc__)
    return opensearch_client