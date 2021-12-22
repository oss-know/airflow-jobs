from tenacity import *
from opensearchpy import OpenSearch
from opensearchpy import helpers as OpenSearchHelpers
import urllib3

github_headers = {'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate, br', 'Accept': '*/*',
                  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36', }


class HttpGetException(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


# retry 防止SSL解密错误，请正确处理是否忽略证书有效性
@retry(stop=stop_after_attempt(3),
       wait=wait_fixed(1),
       retry=retry_if_exception_type(urllib3.exceptions.HTTPError))
def do_get_result(req_session, url, headers, params):
    # 尝试处理网络请求错误
    # session.mount('http://', HTTPAdapter(
    #     max_retries=Retry(total=5, method_whitelist=frozenset(['GET', 'POST']))))  # 设置 post()方法进行重访问
    # session.mount('https://', HTTPAdapter(
    #     max_retries=Retry(total=5, method_whitelist=frozenset(['GET', 'POST']))))  # 设置 post()方法进行重访问
    # print("do_get_result::", params)
    # raise urllib3.exceptions.SSLError('获取github commits 失败！')

    res = req_session.get(url, headers=headers, params=params)
    if res.status_code >= 300:
        print("url:", url)
        print("headers:", headers)
        print("status_code:", res.status_code)
        print("params:", params)
        print("text:", res.text)
        raise HttpGetException('http get 失败！')

    return res


def get_opensearch_client(opensearch_conn_infos):
    client = OpenSearch(
        hosts=[{'host': opensearch_conn_infos["HOST"], 'port': opensearch_conn_infos["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_infos["USER"], opensearch_conn_infos["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )
    return client

# todo : 并发传递数据怎么区分
do_opensearch_bulk_data = None


def return_last_value(retry_state):
    print(retry_state)
    print(retry_state.outcome.result())
    print("需要保存备用的bulk数据", do_opensearch_bulk_data)
    # 得到airflow 的 pqSQL 连接 才能保存
    return retry_state.outcome.result()


# retry 防止SSL解密错误，请正确处理是否忽略证书有效性
# todo: 更新opensearch 错误抓取
@retry(stop=stop_after_attempt(3),
       wait=wait_fixed(1),
       retry_error_callback=return_last_value,
       retry=retry_if_exception_type(urllib3.exceptions.HTTPError))
def do_opensearch_bulk(opensearch_client, bulk_all_data):
    do_opensearch_bulk_data = bulk_all_data
    success, failed = OpenSearchHelpers.bulk(client=opensearch_client, actions=bulk_all_data)
    return success, failed

# --------------------------------------------

PostgresSqlConn = None


def set_postgresSQL_conn(postgresSQL_conn):
    PostgresSqlConn = postgresSQL_conn


def get_postgresSQL_conn():
    return PostgresSqlConn
