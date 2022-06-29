import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, \
    NEED_SYNC_GITHUB_PULL_REPUESTS_REPOS, PROXY_CONFS

# v0.0.1
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(
        dag_id='github_sync_pull_requests_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_sync_github_pull_requests(ds, **kwargs):
        elasticdump_time_point = int(datetime.now().timestamp() * 1000)
        kwargs['ti'].xcom_push(key=f'github_pull_requests_elasticdump_time_point', value=elasticdump_time_point)
        return 'scheduler_sync_github_pull_requests'


    def do_elasticdump_data(**kwargs):
        time.sleep(5)
        from opensearchpy import OpenSearch, helpers
        opensearch_client = OpenSearch(
            hosts=[{'host': "192.168.8.2", 'port': 19201}],
            http_compress=True,
            http_auth=("admin", "admin"),
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False
        )

        elasticdump_time_point = kwargs['ti'].xcom_pull(key=f'github_pull_requests_elasticdump_time_point')
        from oss_know.libs.github.elasticdump import output_script
        from oss_know.libs.github.elasticdump import output_script
        ak_sk = Variable.get("obs_ak_sk", deserialize_json=True)
        ak = ak_sk['ak']
        sk = ak_sk['sk']
        output_script(index='github_pull_requests', time_point=elasticdump_time_point, ak=ak, sk=sk)
        results = helpers.scan(client=opensearch_client, index='github_pull_requests', query={
            "track_total_hits": True,
            "query": {
                "bool": {
                    "must": [
                        {"term": {
                            "search_key.if_sync": {
                                "value": 1
                            }
                        }}, {
                            "range": {
                                "search_key.updated_at": {
                                    "gte": elasticdump_time_point
                                }
                            }
                        }
                    ]
                }
            }
        })
        # print(results)
        print(elasticdump_time_point)
        for result in results:
            print(result)
        return 'do_sync_git_info:::end'

    op_scheduler_sync_github_pull_requests = PythonOperator(
        task_id='op_scheduler_sync_github_pull_requests',
        python_callable=scheduler_sync_github_pull_requests
    )

    op_do_elasticdump_data = PythonOperator(
        task_id=f'do_elasticdump_data',
        python_callable=do_elasticdump_data,

    )


    def do_sync_github_pull_requests(params):
        from airflow.models import Variable
        from oss_know.libs.github import sync_pull_requests

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
        opensearch_conn_infos = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
        proxy_api_url = proxy_confs["api_url"]
        proxy_order_id = proxy_confs["orderid"]
        proxy_reserved_proxies = proxy_confs["reserved_proxies"]
        proxies = []
        for proxy in proxy_reserved_proxies:
            proxies.append(f"http://{proxy}")
        proxy_service = KuaiProxyService(api_url=proxy_api_url,
                                         orderid=proxy_order_id)
        token_manager = TokenManager(tokens=github_tokens)
        proxy_manager = ProxyManager(proxies=proxies,
                                     proxy_service=proxy_service)
        proxy_accommodator = GithubTokenProxyAccommodator(token_manager=token_manager,
                                                          proxy_manager=proxy_manager,
                                                          shuffle=True,
                                                          policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)
        owner = params["owner"]
        repo = params["repo"]

        pull_requests_numbers = sync_pull_requests.sync_github_pull_requests(opensearch_conn_info=opensearch_conn_infos,
                                                                             owner=owner,
                                                                             repo=repo,
                                                                             token_proxy_accommodator=proxy_accommodator)

        return pull_requests_numbers


    # def do_sync_github_pull_requests_comments(params, **kwargs):
    #     owner = params["owner"]
    #     repo = params["repo"]
    #
    #     ti = kwargs['ti']
    #     task_ids = f'op_do_sync_github_pull_requests_{owner}_{repo}'
    #     issues_numbers = ti.xcom_pull(task_ids=task_ids)
    #
    #     from airflow.models import Variable
    #     from libs.github import sync_issues_comments
    #
    #     github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    #     opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    #
    #     do_sync_since = sync_issues_comments.sync_github_issues_comments(
    #         github_tokens, opensearch_conn_info, owner, repo, issues_numbers)
    #
    #
    # def do_sync_github_issues_timelines(params, **kwargs):
    #     owner = params["owner"]
    #     repo = params["repo"]
    #
    #     ti = kwargs['ti']
    #     task_ids = f'op_do_sync_github_issues_{owner}_{repo}'
    #     issues_numbers = ti.xcom_pull(task_ids=task_ids)
    #
    #     from airflow.models import Variable
    #     from libs.github import sync_issues_timelines
    #
    #     github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    #     opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    #
    #     do_sync_since = sync_issues_timelines.sync_github_issues_timelines(
    #         github_tokens=github_tokens, opensearch_conn_info=opensearch_conn_info,
    #         owner=owner, repo=repo, issues_numbers=issues_numbers)

    from airflow.models import Variable

    need_do_sync_ops = []
    need_sync_github_pull_requests_repos = Variable.get(NEED_SYNC_GITHUB_PULL_REPUESTS_REPOS, deserialize_json=True)
    for sync_github_pull_requests_repo in need_sync_github_pull_requests_repos:
        op_do_sync_github_pull_requests = PythonOperator(
            task_id='op_do_sync_github_pull_requests_{owner}_{repo}'.format(
                owner=sync_github_pull_requests_repo["owner"],
                repo=sync_github_pull_requests_repo["repo"]),
            python_callable=do_sync_github_pull_requests,
            op_kwargs={'params': sync_github_pull_requests_repo},
            provide_context=True,
        )

        # op_do_sync_github_pull_requests_comments = PythonOperator(
        #     task_id='op_do_sync_github_issues_comments_{owner}_{repo}'.format(
        #         owner=sync_github_pull_requests_repo["owner"],
        #         repo=sync_github_pull_requests_repo["repo"]),
        #     python_callable=do_sync_github_pull_requests_comments,
        #     op_kwargs={'params': sync_github_pull_requests_repo},
        #     # provide_context=True,
        # )
        #
        # op_do_sync_github_issues_timelines = PythonOperator(
        #     task_id='op_do_sync_github_issues_timelines_{owner}_{repo}'.format(
        #         owner=sync_github_pull_requests_repo["owner"],
        #         repo=sync_github_pull_requests_repo["repo"]),
        #     python_callable=do_sync_github_issues_timelines,
        #     op_kwargs={'params': sync_github_pull_requests_repo},
        #     # provide_context=True,
        # )

        op_scheduler_sync_github_pull_requests >> op_do_sync_github_pull_requests >> op_do_elasticdump_data
        # op_do_sync_github_pull_requests >> op_do_sync_github_pull_requests_comments
        # op_do_sync_github_pull_requests >> op_do_sync_github_issues_timelines
