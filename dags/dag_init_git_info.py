import time
from datetime import datetime
from pprint import pprint
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        dag_id='git_init_sync_v1',
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
        from libs.github import init_gits
        owner = params["owner"]
        repo = params["repo"]
        url = params["url"]
        proxy_config = params.get("proxy_config")
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)

        init_sync_git_info = init_gits.init_sync_git_datas(git_url=url,
                                                      owner=owner,
                                                      repo=repo,
                                                      proxy_config=proxy_config,
                                                      opensearch_conn_datas=opensearch_conn_datas)
        print(init_sync_git_info)
        return 'do_sync_git_info:::end'


    from airflow.models import Variable

    git_info_list = Variable.get("git_info_list", deserialize_json=True)
    for git_info in git_info_list:
        op_do_init_sync_git_info = PythonOperator(
            task_id='do_sync_git_info_{owner}_{repo}'.format(
                owner=git_info["owner"],
                repo=git_info["repo"]),
            python_callable=do_sync_git_info,
            op_kwargs={'params': git_info},
        )

        op_init_sync_git_info >> op_do_init_sync_git_info
