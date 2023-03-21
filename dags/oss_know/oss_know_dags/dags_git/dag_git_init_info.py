from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import NEED_INIT_GITS, OPENSEARCH_CONN_DATA, \
    GIT_SAVE_LOCAL_PATH
from oss_know.libs.github.init_gits import init_gits_repo
from oss_know.libs.util.base import unify_gits_origin

# gits_init_v0.0.3
with DAG(
        dag_id='git_init_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def do_init_gits_repo(owner, repo, origin):
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        git_save_local_path = Variable.get(GIT_SAVE_LOCAL_PATH, deserialize_json=True)
        init_gits_repo(git_url=origin,
                       owner=owner,
                       repo=repo,
                       proxy_config=None,
                       opensearch_conn_datas=opensearch_conn_info,
                       git_save_local_path=git_save_local_path)
        return 'do_sync_git_info:::end'


    git_info_list = Variable.get(NEED_INIT_GITS, deserialize_json=True)
    for git_info in git_info_list:
        owner = git_info["owner"]
        repo = git_info["repo"]
        url = git_info["url"]
        op_do_init_sync_git_info = PythonOperator(
            task_id=f'do_init_gits_{owner}_{repo}',
            python_callable=do_init_gits_repo,
            op_kwargs={
                'owner': owner,
                'repo': repo,
                'origin': unify_gits_origin(url)
            },
        )
