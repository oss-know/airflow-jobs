import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GIT_SAVE_LOCAL_PATH

# git_init_sync_v0.0.3


with DAG(
        dag_id='git_sync_v1',
        # schedule_interval='*/5 * * * *',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def init_sync_git_info(ds, **kwargs):
        elasticdump_time_point = int(datetime.now().timestamp() * 1000)
        kwargs['ti'].xcom_push(key=f'gits_sync_elasticdump_time_point', value=elasticdump_time_point)
        return 'Start init_sync_git_info'


    op_init_sync_git_info = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_sync_git_info,
    )


    def do_sync_git_info(params):
        from airflow.models import Variable
        from oss_know.libs.github import sync_gits
        owner = params["owner"]
        repo = params["repo"]
        url = params["url"]
        proxy_config = params.get("proxy_config")
        opensearch_conn_datas = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        git_save_local_path = Variable.get(GIT_SAVE_LOCAL_PATH, deserialize_json=True)
        sync_git_info = sync_gits.sync_gits_opensearch(git_url=url,
                                                       owner=owner,
                                                       repo=repo,
                                                       proxy_config=proxy_config,
                                                       opensearch_conn_datas=opensearch_conn_datas,
                                                       git_save_local_path=git_save_local_path)
        return 'do_sync_git_info:::end'


    from airflow.models import Variable

    git_info_list = Variable.get("need_sync_gits", deserialize_json=True)
    for git_info in git_info_list:
        op_do_init_sync_git_info = PythonOperator(
            task_id=f'do_sync_git_info_{git_info["owner"]}_{git_info["repo"]}',
            python_callable=do_sync_git_info,
            op_kwargs={'params': git_info},
        )
        op_init_sync_git_info >> op_do_init_sync_git_info
