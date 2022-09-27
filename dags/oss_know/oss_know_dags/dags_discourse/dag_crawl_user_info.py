from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch, helpers

from oss_know.libs.base_dict.variable_key import NEED_INIT_DISCOURSE

with DAG(
    dag_id = 'discourse_crawl_user_info',
    schedule_interval=None,
    start_date=datetime(2022, 9, 26),
    catchup=False,
    tags=['discourse']
) as dag:
    
    def scheduler_init_crawl_discourse_user_info(ds, **kwargs):
        return 'End::scheduler_init_crawl_discourse_user_info'


    op_scheduler_init_crawl_discourse_user_info = PythonOperator(
        task_id='scheduler_init_crawl_discourse_user_info',
        python_callable=scheduler_init_crawl_discourse_user_info
    )

    def do_crawl_discourse_user_info(params):
        from airflow.models import Variable
        from oss_know.libs.discourse import crawl_user_info
        owner = params["owner"]
        repo  = params["repo"]
        url   = params["url"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        crawl_user_info_info = crawl_user_info.crawl_user_info(base_url=url,
                                                                        owner=owner,
                                                                        repo=repo,
                                                                        opensearch_conn_datas=opensearch_conn_datas)
        return 'do_crawl_discourse_user_info:::end'

    def do_crawl_discourse_user_action(params):
        from airflow.models import Variable
        from oss_know.libs.discourse import crawl_user_action
        owner = params["owner"]
        repo  = params["repo"]
        url   = params["url"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        crawl_user_action_info = crawl_user_action.crawl_user_action(base_url=url,
                                                                            owner=owner,
                                                                            repo=repo,
                                                                            opensearch_conn_datas=opensearch_conn_datas)
        return 'do_crawl_discourse_user_action:::end'


    from airflow.models import Variable

    discourse_info_list = Variable.get(NEED_INIT_DISCOURSE, deserialize_json=True)
    for discourse_info in discourse_info_list:

        op_do_crawl_discourse_user_info = PythonOperator(
            task_id=f'do_crawl_discourse_user_info_{discourse_info["owner"]}_{discourse_info["repo"]}',
            python_callable=do_crawl_discourse_user_info,
            op_kwargs={'params': discourse_info}
        )

        op_do_crawl_discourse_user_action = PythonOperator(
            task_id=f'do_crawl_discourse_user_action_{discourse_info["owner"]}_{discourse_info["repo"]}',
            python_callable=do_crawl_discourse_user_action,
            op_kwargs={'params': discourse_info}
        )

        # op_scheduler_init_crawl_discourse_user_info >> op_do_crawl_discourse_user_info 
        op_do_crawl_discourse_user_info >> op_do_crawl_discourse_user_action
