import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from loguru import logger

with DAG(
        dag_id='github_init_profile_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def start_load_github_profile(ds, **kwargs):
        return 'End start_load_github_profile'


    op_start_load_github_profile = PythonOperator(
        task_id='load_github_profile_info',
        python_callable=start_load_github_profile,
    )


    def load_github_repo_login(params):
        from airflow.models import Variable

        from libs.github import init_profile_by_github_users_login_set

        github_tokens = Variable.get("github_tokens", deserialize_json=True)
        opensearch_conn_infos = Variable.get("opensearch_conn_data", deserialize_json=True)

        owner = params["owner"]
        repo = params["repo"]
        init_profile_dict = init_profile_by_github_users_login_set.load_github_repo_github_user_login(github_tokens, opensearch_conn_infos, owner, repo)

        # todo: need clean just for test
        # do_add_updated_github_profiles = init_profiles_by_github_commits.add_updated_github_profiles(github_tokens,
        # opensearch_conn_infos)
        logger.info(f'init_profile_dict:{init_profile_dict}')
        print('LANCE DEBUG', init_profile_dict)
        # return init_profile_dict
        return {'foo': list(init_profile_dict['logins'])}


    def load_github_repo_profile(**kwargs):
        print('LANCE DEBUG', kwargs)
        # login_set = kwargs['login']
        # github_users_login = login_set.xcom_pull(task_ids='op_load_github_repo_profile_{owner}_{repo}'.format(
        #     owner=now_need_sync_github_profile_repos["owner"],
        #     repo=now_need_sync_github_profile_repos["repo"]),
        #     python_callable=load_github_repo_profile,
        #     op_kwargs={'params': now_need_sync_github_profile_repos})
        # from libs.github import init_profile
        # init_profile.load_github_profile(github_users_login)
        return 'End load_github_repo_profile'


    need_sync_github_profile_repos = Variable.get("need_sync_github_profile_repo_list", deserialize_json=True)

    for now_need_sync_github_profile_repos in need_sync_github_profile_repos:
        op_load_github_repo_login = PythonOperator(
            task_id='op_load_github_repo_login_{owner}_{repo}'.format(
                owner=now_need_sync_github_profile_repos["owner"],
                repo=now_need_sync_github_profile_repos["repo"]),
            python_callable=load_github_repo_login,
            op_kwargs={'params': now_need_sync_github_profile_repos},
        )
        op_load_github_repo_profile = PythonOperator(
            task_id='op_load_github_repo_profile_{owner}_{repo}'.format(
                owner=now_need_sync_github_profile_repos["owner"],
                repo=now_need_sync_github_profile_repos["repo"]),
            python_callable=load_github_repo_profile,
            op_kwargs={'params': now_need_sync_github_profile_repos},
        )

        op_start_load_github_profile >> op_load_github_repo_login >> op_load_github_repo_profile
        # op_start_load_github_profile >> op_load_github_repo_login