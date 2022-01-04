from airflow.utils.db import provide_session
from airflow.models import XCom
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITHUB_PROFILES_REPOS


@provide_session
def cleanup_xcom(session=None):
    dag_id = 'github_init_profile_v1'
    print("===========================================testXCom")
    print(XCom.dag_id)
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


    def load_github_repo_login(params, **kwargs):
        from airflow.models import Variable
        from oss_know.libs.github import init_profiles
        from oss_know.libs.github import sync_profiles
        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)
        owner = params["owner"]
        repo = params["repo"]
        init_logins = init_profiles.load_github_logins_by_repo(opensearch_conn_infos, owner, repo)
        kwargs['ti'].xcom_push(key=f'{owner}_{repo}_logins', value=init_logins)
        # todo: need clean just for test
        # github_tokens = Variable.get("github_tokens", deserialize_json=True)
        # do_add_updated_github_profiles = sync_profiles.add_updated_github_profiles(github_tokens,
        # opensearch_conn_infos)


    def load_github_repo_profile(params, **kwargs):
        from airflow.models import Variable
        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)
        github_users_logins =[]
        for param in params:
            owner = param["owner"]
            repo = param["repo"]
            github_users_logins += kwargs['ti'].xcom_pull(key=f'{owner}_{repo}_logins')
        from oss_know.libs.github import init_profiles
        init_profiles.load_github_profiles(github_tokens=github_tokens, opensearch_conn_infos=opensearch_conn_infos,
                                           github_users_logins=github_users_logins)
        return 'End load_github_repo_profile'


    need_sync_github_profile_repos = Variable.get(NEED_INIT_GITHUB_PROFILES_REPOS, deserialize_json=True)

    op_load_github_repo_profile = PythonOperator(
        task_id='op_load_github_repo_profiles',
        python_callable=load_github_repo_profile,
        op_kwargs={'params': need_sync_github_profile_repos},
        provide_context=True
    )
    for now_need_sync_github_profile_repos in need_sync_github_profile_repos:
        op_load_github_repo_login = PythonOperator(
            task_id='op_load_github_repo_login_{owner}_{repo}'.format(
                owner=now_need_sync_github_profile_repos["owner"],
                repo=now_need_sync_github_profile_repos["repo"]),
            python_callable=load_github_repo_login,
            op_kwargs={'params': now_need_sync_github_profile_repos},
            provide_context=True
        )
        op_start_load_github_profile >> op_load_github_repo_login >> op_load_github_repo_profile
