import random
import time
import requests
import datetime
import numpy as np
import json
from threading import Thread

from opensearchpy import OpenSearch
from opensearchpy import helpers as opensearch_helpers

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES, \
    OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.exceptions import GithubResourceNotFoundError
from oss_know.libs.util.github_api import GithubAPI, GithubException
from oss_know.libs.util.log import logger
from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.base_dict.options import GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX


def init_sync_github_issues_timeline(opensearch_conn_info, owner, repo, token_proxy_accommodator, since=None):
    opensearch_client = OpenSearch(
        hosts=[{'host': opensearch_conn_info["HOST"], 'port': opensearch_conn_info["PORT"]}],
        http_compress=True,
        http_auth=(opensearch_conn_info["USER"], opensearch_conn_info["PASSWD"]),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
    )

    # 根据指定的 owner/repo , 获取现在所有的 issues，并根据所有 issues 便利相关的 comments
    scan_results = opensearch_helpers.scan(opensearch_client,
                                           index=OPENSEARCH_INDEX_GITHUB_ISSUES,
                                           query={
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
                                           },
                                           doc_type="_doc"
                                           )
    need_init_all_results = []
    for now_item in scan_results:
        need_init_all_results.append(now_item)

    # 不要在dag or task里面 创建index 会有并发异常！！！
    # if not opensearch_client.indices.exists("github_issues"):
    #     opensearch_client.indices.create("github_issues")

    # 由于需要初始化幂等要求，在重新初始化前删除对应owner/repo 指定的 issues_timeline 记录的所有数据
    del_result = opensearch_client.delete_by_query(index=OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
                                                   body={
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

    logger.info(f"DELETE github issues_timeline result:", del_result)

    get_timeline_tasks = list()
    get_timeline_results = list()
    get_timeline_fails_results = list()
    for idx, now_item in enumerate(need_init_all_results):
        number = now_item["_source"]["raw_data"]["number"]

        # 创建并发任务
        ct = concurrent_threads(do_get_github_timeline, args=(opensearch_client, token_proxy_accommodator, owner, repo, number))
        get_timeline_tasks.append(ct)
        ct.start()

        # 执行并发任务并获取结果
        if idx % 20 == 0 :
            for tt in get_timeline_tasks:
                tt.join()
                if tt.getResult()[0] != 200:
                    get_timeline_fails_results.append(tt.getResult())

                get_timeline_results.append(tt.getResult())
        if len(get_timeline_fails_results) != 0:
            raise GithubException('github请求失败！', get_timeline_fails_results)

# 支持return值的线程
class concurrent_threads(Thread):
    def __init__(self, func, args=()):
        '''
        :param func: 被测试的函数
        :param args: 被测试的函数的返回值
        '''
        super(concurrent_threads, self).__init__()
        self.func = func
        self.args = args

    def run(self) -> None:
        self.result = self.func(*self.args)

    def getResult(self):
        try:
            return self.result
        except BaseException as e:
            return e.args[0]

# 在concurrent_threads中执行的具体的业务方法
def do_get_github_timeline(opensearch_client, token_proxy_accommodator, owner, repo, number):
    req_session = requests.Session()
    github_api = GithubAPI()
    opensearch_api = OpensearchAPI()

    for page in range(1, 10000):
        time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))
        try:
            req = github_api.get_github_issues_timeline(
                req_session, token_proxy_accommodator, owner, repo, number, page)
            one_page_github_issues_timeline = req.json()
        except GithubResourceNotFoundError as e:
            logger.error(f"fail init github timeline, {owner}/{repo}, issues_number:{number}, now_page:{page}, Target timeline info does not exist: {e}, end")
            return 403, e

        if (one_page_github_issues_timeline is not None) and len(
                one_page_github_issues_timeline) == 0:
            logger.info(f"success init github timeline, {owner}/{repo}, issues_number:{number}, page_count:{page}, end")
            return 200, f"success init github timeline, {owner}/{repo}, issues_number:{number}, page_count:{page}, end"

        opensearch_api.bulk_github_issues_timeline(opensearch_client=opensearch_client,
                                                   issues_timelines=one_page_github_issues_timeline,
                                                   owner=owner, repo=repo, number=number)

        logger.info(f"success get github timeline, {owner}/{repo}, issues_number:{number}, page_index:{page}, end")

    # '''
    # :param code: 状态码
    # :param seconds: 请求响应时间
    # :return:
    # '''
    # r = requests.get(url='http://www.baidu.com/')
    # code = r.status_code
    # seconds = r.elapsed.total_seconds()
    # return code, seconds


# def calculation_time(startTime, endTime):
#     '''计算两个时间之差，单位是秒'''
#     return (endTime - startTime).seconds


# def getResult(seconds):
#     '''获取服务端的响应时间信息'''
#     data = {
#         'Max': sorted(seconds)[-1],
#         'Min': sorted(seconds)[0],
#         'Median': np.median(seconds),
#         '99%Line': np.percentile(seconds, 99),
#         '95%Line': np.percentile(seconds, 95),
#         '90%Line': np.percentile(seconds, 90)
#     }
#     return data


# def highConcurrent(count):
#     '''
#     对服务端发送高并发的请求
#     :param cout: 并发数
#     :return:
#     '''
#     startTime = datetime.datetime.now()
#     sum = 0
#     list_count = list()
#     tasks = list()
#     results = list()
#     # 失败的信息
#     fails = []
#     # 成功任务数
#     success = []
#     codes = list()
#     seconds = list()
#
#     # 创建并发任务
#     for i in range(1, count):
#         t = concurrent_threads(do_get_github_timeline, args=(i, i))
#         tasks.append(t)
#         t.start()
#
#     # 执行并发任务并获取结果
#     for t in tasks:
#         t.join()
#         if t.getResult()[0] != 200:
#             fails.append(t.getResult())
#         results.append(t.getResult())
#
#     endTime = datetime.datetime.now()
#     for item in results:
#         codes.append(item[0])
#         seconds.append(item[1])
#     for i in range(len(codes)):
#         list_count.append(i)
#
#     # #生成可视化的趋势图
#     # fig,ax=plt.subplots()
#     # ax.plot(list_count,seconds)
#     # ax.set(xlabel='number of times', ylabel='Request time-consuming',
#     #        title='olap continuous request response time (seconds)')
#     # ax.grid()
#     # fig.savefig('olap.png')
#     # plt.show()
#
#     for i in seconds:
#         sum += i
#     rate = sum / len(list_count)
#     # print('\n总共持续时间:\n',endTime-startTime)
#     totalTime = calculation_time(startTime=startTime, endTime=endTime)
#     if totalTime < 1:
#         totalTime = 1
#     # 吞吐量的计算
#     try:
#         throughput = int(len(list_count) / totalTime)
#     except Exception as e:
#         print(e.args[0])
#     getResult(seconds=seconds)
#     errorRate = 0
#     if len(fails) == 0:
#         errorRate = 0.00
#     else:
#         errorRate = len(fails) / len(tasks) * 100
#     throughput = str(throughput) + '/S'
#     timeData = getResult(seconds=seconds)
#     dict1 = {
#         '吞吐量': throughput,
#         '平均响应时间': rate,
#         '响应时间': timeData,
#         '错误率': errorRate,
#         '请求总数': len(list_count),
#         '失败数': len(fails)
#     }
#     return json.dumps(dict1, indent=True, ensure_ascii=False)


# if __name__ == '__main__':
#     print(highConcurrent(count=1000))

        # for page in range(1, 10000):
        #     time.sleep(random.uniform(GITHUB_SLEEP_TIME_MIN, GITHUB_SLEEP_TIME_MAX))
        #     try:
        #         req = github_api.get_github_issues_timeline(req_session, token_proxy_accommodator, owner, repo, number,
        #                                                     page)
        #         one_page_github_issues_timeline = req.json()
        #     except GithubResourceNotFoundError as e:
        #         logger.error(f'Target timeline info does not exist: {e}')
        #         break
        #
        #     if (one_page_github_issues_timeline is not None) and len(
        #             one_page_github_issues_timeline) == 0:
        #         logger.info(f"init sync github issues end to break:{owner}/{repo} page_index:{page}")
        #         break
        #
        #     opensearch_api.bulk_github_issues_timeline(opensearch_client=opensearch_client,
        #                                                issues_timelines=one_page_github_issues_timeline,
        #                                                owner=owner, repo=repo, number=number)
        #
        #     logger.info(f"success get github issues page:{owner}/{repo} page_index:{page}")


# def get_github_issues_timeline(req_session, github_tokens_iter, owner, repo, number, page,
#                                since):
#     url = "https://api.github.com/repos/{owner}/{repo}/issues/{number}/timeline".format(
#         owner=owner, repo=repo, number=number)
#     headers = copy.deepcopy(github_headers)
#     headers.update({'Authorization': 'token %s' % next(github_tokens_iter)})
#     params = {'per_page': 100, 'page': page, 'since': since}
#     res = do_get_result(req_session, url, headers, params)
#     return res
#
#
# def bulk_github_pull_issues_timeline(now_github_issues_timeline, opensearch_client, owner, repo, number):
#     bulk_all_datas = []
#
#     for val in now_github_issues_timeline:
#         template = {"_index": OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE,
#                     "_source": {"search_key": {"owner": owner, "repo": repo, "number": number},
#                                 "raw_data": None}}
#         append_item = copy.deepcopy(template)
#         append_item["_source"]["raw_data"] = val
#         bulk_all_datas.append(append_item)
#         logger.info(f"add init sync github issues_timeline number:{number}")
#
#     success, failed = opensearch_helpers.bulk(client=opensearch_client, actions=bulk_all_datas)
#     logger.info(f"now page:{len(bulk_all_datas)} sync github issues_timeline success:{success} & failed:{failed}")
