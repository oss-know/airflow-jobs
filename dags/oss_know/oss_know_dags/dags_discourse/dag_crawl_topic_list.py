from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch, helpers

from oss_know.libs.base_dict.variable_key import NEED_INIT_DISCOURSE

with DAG(
    dag_id = 'discourse_crawl_topic_list',
    schedule_interval=None,
    start_date=datetime(2022, 9, 24),
    catchup=False,
    tags=['discourse']
) as dag:
    
    def scheduler_init_crawl_discourse_topic_list(ds, **kwargs):
        return 'End::scheduler_init_crawl_discourse_topic_list'


    op_scheduler_init_crawl_discourse_topic_list = PythonOperator(
        task_id='scheduler_init_crawl_discourse_topic_list',
        python_callable=scheduler_init_crawl_discourse_topic_list
    )

    def do_crawl_discourse_topic_list(params):
        from airflow.models import Variable
        from oss_know.libs.discourse import crawl_topic_list
        owner = params["owner"]
        repo  = params["repo"]
        url   = params["url"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        crawl_discourse_topic_list_info = crawl_topic_list.crawl_topic_list(base_url=url,
                                                                            owner=owner,
                                                                            repo=repo,
                                                                            opensearch_conn_datas=opensearch_conn_datas)
        return 'do_crawl_discourse_topic_list:::end'

    from airflow.models import Variable

    discourse_info_list = Variable.get(NEED_INIT_DISCOURSE, deserialize_json=True)
    for discourse_info in discourse_info_list:
        op_do_crawl_discourse_topic_list = PythonOperator(
            task_id=f'do_crawl_discourse_topic_list_{discourse_info["owner"]}_{discourse_info["repo"]}',
            python_callable=do_crawl_discourse_topic_list,
            op_kwargs={'params': discourse_info}
        )

        op_scheduler_init_crawl_discourse_topic_list >>op_do_crawl_discourse_topic_list
