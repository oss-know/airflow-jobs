from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch, helpers
# git_init_sync_v0.0.3
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITS

with DAG(
        dag_id='git_init_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def init_sync_git_info(ds, **kwargs):
        return 'Start init_sync_git_info'


    op_init_sync_git_info = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_sync_git_info,
    )


    def do_sync_git_info(params):
        from airflow.models import Variable
        from oss_know.libs.github import init_gits
        owner = params["owner"]
        repo = params["repo"]
        url = params["url"]
        proxy_config = params.get("proxy_config")
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
        init_sync_git_info = init_gits.init_sync_git_datas(git_url=url,
                                                           owner=owner,
                                                           repo=repo,
                                                           proxy_config=proxy_config,
                                                           opensearch_conn_datas=opensearch_conn_datas,
                                                           git_save_local_path=git_save_local_path)
        return 'do_sync_git_info:::end'


    from airflow.models import Variable

    git_info_list = Variable.get(NEED_INIT_GITS, deserialize_json=True)

    for git_info in git_info_list:
        op_do_init_sync_git_info = PythonOperator(
            task_id=f'do_sync_git_info_{git_info["owner"]}_{git_info["repo"]}',
            python_callable=do_sync_git_info,
            op_kwargs={'params': git_info},
        )

        op_init_sync_git_info >>op_do_init_sync_git_info
