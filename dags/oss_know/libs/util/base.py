import re

import redis
import requests
import urllib3
from geopy.geocoders import GoogleV3
from multidict import CIMultiDict
from opensearchpy import OpenSearch
from tenacity import *

from oss_know.libs.base_dict.infer_file import CCTLD, COMPANY_COUNTRY
from oss_know.libs.base_dict.variable_key import LOCATIONGEO_TOKEN
from ..util.log import logger


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
    # raise urllib3.exceptions.SSLError('获取github commits 失败！')

    res = req_session.get(url, headers=headers, params=params)
    if res.status_code >= 300:
        logger.warning(f"url:{url}")
        logger.warning(f"headers:{headers}")
        logger.warning(f"params:{params}")
        logger.warning(f"text:{res.text}")
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


def get_redis_client(redis_client_info):
    redis_client = redis.Redis(host=redis_client_info["HOST"], port=redis_client_info["PORT"], db=0,
                               decode_responses=True)
    return redis_client


def infer_country_from_emailcctld(email):
    """
        :param  company: the company given by github
        :return country_name  : the english name of a country
        """
    profile_domain = email.split(".")[-1].upper()
    if profile_domain in CCTLD:
        return CCTLD[profile_domain]
    return None


def infer_country_from_emaildomain(email):
    """
        :param  company: the company given by github
        :return country_name  : the english name of a country
        """
    emaildomain = str(re.findall(r"@(.+?)\.", email))
    if emaildomain in COMPANY_COUNTRY:
        return COMPANY_COUNTRY[emaildomain]
    return None

def infer_company_from_emaildomain(email):
    """
        :param  company: the company given by github
        :return country_name  : the english name of a country
        """
    emaildomain = str(re.findall(r"@(.+?)\.", email))
    if emaildomain in COMPANY_COUNTRY:
        return emaildomain
    return None


def infer_country_from_location(githubLocation):
    """
        :param  githubLocation: the location given by github
        :return country_name  : the english name of a country
        """
    from airflow.models import Variable
    api_token = Variable.get(LOCATIONGEO_TOKEN, deserialize_json=True)
    geolocator = GoogleV3(api_key=api_token)
    geo_res = geolocator.geocode(githubLocation, language='en')
    if geo_res:
        return geo_res.address.split(',')[-1].strip()
    return None


def infer_all_info_from_location(githubLocation):
    """
           :param  githubLocation: the location given by github
           :return country_name  : the english name of a country
           """
    from airflow.models import Variable
    api_token = Variable.get(LOCATIONGEO_TOKEN, deserialize_json=True)
    geolocator = GoogleV3(api_key=api_token)
    geo_res = geolocator.geocode(githubLocation, language='en')
    if geo_res:
        return geo_res.address
    return None



def infer_country_from_company(company):
    """
        :param  company: the company given by github
        :return country_name  : the english name of a country
        """
    company = company.replace("@", " ").lower().strip()
    company_country = CIMultiDict(COMPANY_COUNTRY)
    if company in company_country:
        return company_country[company]
    return None


inferrers = [
        ("country_inferred_from_email_cctld", "email", infer_country_from_emailcctld),
        ("country_inferred_from_email_domain_company", "email", infer_country_from_emaildomain),
        ("country_inferred_from_location", "location", infer_country_from_location),
        ("country_inferred_from_company", "company", infer_country_from_company),
        ("company_inferred_from_email_domain_company", "email", infer_company_from_emaildomain),
        ("inferred_from_location", "location", infer_all_info_from_location)
    ]


def infer_country_insert_into_profile(latest_github_profile):

    try:
        for tup in inferrers:
            key, original_key, infer = tup
            original_property = latest_github_profile[original_key]
            latest_github_profile[key] = infer(original_property) if original_property else None

    except (urllib3.exceptions.MaxRetryError, requests.exceptions.ProxyError) as error:
        logger.error(f"error occurs when inferring country, {error}")
        for inferrer in inferrers:
            latest_github_profile[inferrer[0]] = None
   
