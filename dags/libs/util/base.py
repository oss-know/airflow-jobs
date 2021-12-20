from tenacity import *
from opensearchpy import OpenSearch
import urllib3

github_headers = {'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate, br', 'Accept': '*/*',
                  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36', }


# retry 防止SSL解密错误，请正确处理是否忽略证书有效性
@retry(stop=stop_after_attempt(3),
       wait=wait_fixed(1),
       retry=retry_if_exception_type(urllib3.exceptions.SSLError))
def do_get_result(session, url, headers, params):
    # 尝试处理网络请求错误
    # session.mount('http://', HTTPAdapter(
    #     max_retries=Retry(total=5, method_whitelist=frozenset(['GET', 'POST']))))  # 设置 post()方法进行重访问
    # session.mount('https://', HTTPAdapter(
    #     max_retries=Retry(total=5, method_whitelist=frozenset(['GET', 'POST']))))  # 设置 post()方法进行重访问
    # print("do_get_result::", params)
    # raise urllib3.exceptions.SSLError('获取github commits 失败！')

    res = session.get(url, headers=headers, params=params)
    if res.status_code != 200:
        print("url:", url)
        print("headers:", headers)
        print("params:", params)
        print("text:", res.text)
        raise Exception('获取github commits 失败！')
    return res


def get_client():
    from airflow.models import Variable
    opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)
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
