from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models import XCom
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session

from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_PROFILES_REPOS, LOCATIONGEO_TOKEN
from oss_know.libs.util.proxy import KuaiProxyService, ProxyManager, GithubTokenProxyAccommodator
from oss_know.libs.util.token import TokenManager


@provide_session
def cleanup_xcom(session=None):
    dag_id = 'github_init_profile_v1'
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG(
        dag_id='github_init_profile_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
        on_success_callback=cleanup_xcom
) as dag:
    def start_load_github_profile(ds, **kwargs):
        return 'End start_load_github_profile'


    op_start_load_github_profile = PythonOperator(
        task_id='load_github_profile_info',
        python_callable=start_load_github_profile,
        provide_context=True
    )


    def load_github_repo_id(params, **kwargs):
        from airflow.models import Variable
        from oss_know.libs.github import init_profiles
        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)
        owner = params["owner"]
        repo = params["repo"]
        init_ids = init_profiles.load_github_ids_by_repo(opensearch_conn_infos, owner, repo)
        kwargs['ti'].xcom_push(key=f'{owner}_{repo}_ids', value=init_ids)


    def load_github_repo_profiles(params, **kwargs):
        from airflow.models import Variable
        from oss_know.libs.util.base import init_geolocator

        geolocator_token = Variable.get(LOCATIONGEO_TOKEN, deserialize_json=Falsez)
        init_geolocator(geolocator_token)

        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)

        proxy_confs = Variable.get('proxy_confs', deserialize_json=True)
        proxies = []
        for line in proxy_confs['reserved_proxies']:
            proxies.append(f'http://{line}')

        proxy_service = KuaiProxyService(proxy_confs['api_url'], proxy_confs['orderid'])
        proxy_manager = ProxyManager(proxies, proxy_service)
        token_manager = TokenManager(github_tokens)

        proxy_accommodator = GithubTokenProxyAccommodator(token_manager, proxy_manager, shuffle=True,
                                                          policy=GithubTokenProxyAccommodator.POLICY_FIXED_MAP)

        # github_users_ids = []
        github_users_ids = set()
        for param in params:
            owner = param["owner"]
            repo = param["repo"]
            # github_users_ids += kwargs['ti'].xcom_pull(key=f'{owner}_{repo}_ids')
            # TODO Not very sure if updating the id set with a smaller list(github ids of a repo) performs better
            # TODO Need more explorations on this
            github_users_ids.update(kwargs['ti'].xcom_pull(key=f'{owner}_{repo}_ids'))
        from oss_know.libs.github import init_profiles

        init_profiles.load_github_profiles(token_proxy_accommodator=proxy_accommodator,
                                           opensearch_conn_infos=opensearch_conn_infos,
                                           github_users_ids=github_users_ids)
        return 'End load_github_repo_profile'


    need_sync_github_profile_repos = Variable.get(NEED_INIT_GITHUB_PROFILES_REPOS, deserialize_json=True)

    op_load_github_repo_profiles = PythonOperator(
        task_id='op_load_github_repo_profiles',
        python_callable=load_github_repo_profiles,
        op_kwargs={'params': need_sync_github_profile_repos},
        provide_context=True
    )

    for now_need_sync_github_profile_repos in need_sync_github_profile_repos:
        owner = now_need_sync_github_profile_repos['owner']
        repo = now_need_sync_github_profile_repos['repo']
        op_load_github_repo_id = PythonOperator(
            task_id=f'op_load_github_repo_id_{owner}_{repo}',
            python_callable=load_github_repo_id,
            op_kwargs={'params': now_need_sync_github_profile_repos},
            provide_context=True
        )
        op_start_load_github_profile >> op_load_github_repo_id >> op_load_github_repo_profiles
