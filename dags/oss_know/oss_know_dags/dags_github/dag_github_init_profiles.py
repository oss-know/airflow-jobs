from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models import XCom
from airflow.operators.python import PythonOperator
from airflow.utils.db import provide_session

from oss_know.libs.base_dict.variable_key import GITHUB_TOKENS, LOCATIONGEO_TOKEN, \
    PROXY_CONFS, OPENSEARCH_CONN_DATA, NEED_INIT_GITHUB_PROFILES_REPOS
from oss_know.libs.github import init_profiles
from oss_know.libs.util.base import init_geolocator
from oss_know.libs.util.proxy import GithubTokenProxyAccommodator, make_accommodator, \
    ProxyServiceProvider


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
    def start_load_github_profile():
        return 'End start_load_github_profile'


    op_start_load_github_profile = PythonOperator(
        task_id='load_github_profile_info',
        python_callable=start_load_github_profile,
        provide_context=True
    )

    geolocator_token = Variable.get(LOCATIONGEO_TOKEN, deserialize_json=False)
    init_geolocator(geolocator_token)

    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

    github_tokens = Variable.get(GITHUB_TOKENS, deserialize_json=True)
    proxy_confs = Variable.get(PROXY_CONFS, deserialize_json=True)
    proxy_accommodator = make_accommodator(github_tokens, proxy_confs, ProxyServiceProvider.Kuai,
                                           GithubTokenProxyAccommodator.POLICY_FIXED_MAP)


    def load_github_repo_id(owner, repo, **kwargs):
        init_ids = init_profiles.load_github_ids_by_repo(opensearch_conn_info, owner, repo)
        kwargs['ti'].xcom_push(key=f'{owner}_{repo}_ids', value=init_ids)


    def load_github_repo_profiles(params, **kwargs):
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
        if_sync, if_new_person = 0, 1
        init_profiles.load_github_profiles(token_proxy_accommodator=proxy_accommodator,
                                           opensearch_conn_info=opensearch_conn_info,
                                           github_users_ids=github_users_ids,
                                           if_sync=if_sync,
                                           if_new_person=if_new_person)
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
            op_kwargs={
                'owner': owner,
                'repo': repo,
            },
            provide_context=True
        )
        op_start_load_github_profile >> op_load_github_repo_id >> op_load_github_repo_profiles
