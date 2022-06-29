import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, NEED_SYNC_GITHUB_ISSUES_REPOS, \
    PROXY_CONFS

# v0.0.1
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager

with DAG(
        dag_id='github_sync_issues_v1',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def scheduler_sync_github_issues(ds, **kwargs):
        elasticdump_time_point = int(datetime.now().timestamp() * 1000)
        kwargs['ti'].xcom_push(key=f'github_issues_elasticdump_time_point', value=elasticdump_time_point)
        return 'End:scheduler_sync_github_issues'


    op_scheduler_sync_github_issues = PythonOperator(
        task_id='op_scheduler_sync_github_issues',
        python_callable=scheduler_sync_github_issues
    )


    def do_elasticdump_data(params,**kwargs):
        index = params
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

        elasticdump_time_point = kwargs['ti'].xcom_pull(key=f'github_issues_elasticdump_time_point')
        indices = ['github_issues', 'github_issues_timeline', 'github_issues_comments']
        from oss_know.libs.github.elasticdump import output_script
        ak_sk = Variable.get("obs_ak_sk", deserialize_json=True)
        ak = ak_sk['ak']
        sk = ak_sk['sk']

        output_script(index=index, time_point=elasticdump_time_point, ak=ak, sk=sk)
        results = helpers.scan(client=opensearch_client, index=index, query={
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


    def get_proxy_accommodator():
        from airflow.models import Variable
        from oss_know.libs.github import sync_issues

        github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
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
        return proxy_accommodator


    def do_sync_github_issues(params):
        from airflow.models import Variable
        from oss_know.libs.github import sync_issues
        opensearch_conn_infos = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        proxy_accommodator = get_proxy_accommodator()
        owner = params["owner"]
        repo = params["repo"]

        issues_numbers, pr_numbers = sync_issues.sync_github_issues(opensearch_conn_info=opensearch_conn_infos,
                                                                    owner=owner,
                                                                    repo=repo,
                                                                    token_proxy_accommodator=proxy_accommodator)

        return issues_numbers


    def do_sync_github_issues_comments(params, **kwargs):
        owner = params["owner"]
        repo = params["repo"]

        ti = kwargs['ti']
        task_ids = f'op_do_sync_github_issues_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        from airflow.models import Variable
        from oss_know.libs.github import sync_issues_comments

        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        proxy_accommodator = get_proxy_accommodator()
        do_sync_since = sync_issues_comments.sync_github_issues_comments(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers
        )


    def do_sync_github_issues_timelines(params, **kwargs):
        owner = params["owner"]
        repo = params["repo"]

        ti = kwargs['ti']
        task_ids = f'op_do_sync_github_issues_{owner}_{repo}'
        issues_numbers = ti.xcom_pull(task_ids=task_ids)

        from airflow.models import Variable
        from oss_know.libs.github import sync_issues_timelines

        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        proxy_accommodator = get_proxy_accommodator()
        do_sync_since = sync_issues_timelines.sync_github_issues_timelines(
            opensearch_conn_info=opensearch_conn_info,
            owner=owner,
            repo=repo,
            token_proxy_accommodator=proxy_accommodator,
            issues_numbers=issues_numbers)


    op_do_elasticdump_issues_data = PythonOperator(
        task_id=f'do_elasticdump_issues_data',
        python_callable=do_elasticdump_data,
        op_kwargs={'params': 'github_issues'}

    )

    op_do_elasticdump_issues_comments_data = PythonOperator(
        task_id=f'do_elasticdump_issues_comments_data',
        python_callable=do_elasticdump_data,
        op_kwargs={'params': 'github_issues_comments'}

    )

    op_do_elasticdump_issues_timeline_data = PythonOperator(
        task_id=f'do_elasticdump_issues_timeline_data',
        python_callable=do_elasticdump_data,
        op_kwargs={'params': 'github_issues_timeline'}

    )

    from airflow.models import Variable

    need_do_sync_ops = []
    need_sync_github_issues_repos = Variable.get(NEED_SYNC_GITHUB_ISSUES_REPOS, deserialize_json=True)
    for sync_github_issues_repo in need_sync_github_issues_repos:
        op_do_sync_github_issues = PythonOperator(
            task_id='op_do_sync_github_issues_{owner}_{repo}'.format(
                owner=sync_github_issues_repo["owner"],
                repo=sync_github_issues_repo["repo"]),
            python_callable=do_sync_github_issues,
            op_kwargs={'params': sync_github_issues_repo},
            provide_context=True,
        )

        op_do_sync_github_issues_comments = PythonOperator(
            task_id='op_do_sync_github_issues_comments_{owner}_{repo}'.format(
                owner=sync_github_issues_repo["owner"],
                repo=sync_github_issues_repo["repo"]),
            python_callable=do_sync_github_issues_comments,
            op_kwargs={'params': sync_github_issues_repo},
            # provide_context=True,
        )

        op_do_sync_github_issues_timelines = PythonOperator(
            task_id='op_do_sync_github_issues_timelines_{owner}_{repo}'.format(
                owner=sync_github_issues_repo["owner"],
                repo=sync_github_issues_repo["repo"]),
            python_callable=do_sync_github_issues_timelines,
            op_kwargs={'params': sync_github_issues_repo},
            # provide_context=True,
        )

        op_scheduler_sync_github_issues >> op_do_sync_github_issues
        op_do_sync_github_issues >> op_do_elasticdump_issues_data
        op_do_sync_github_issues >> op_do_sync_github_issues_comments >> op_do_elasticdump_issues_comments_data
        op_do_sync_github_issues >> op_do_sync_github_issues_timelines >> op_do_elasticdump_issues_timeline_data
