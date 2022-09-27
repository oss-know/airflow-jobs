from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch, helpers

from oss_know.libs.base_dict.variable_key import NEED_INIT_DISCOURSE

with DAG(
    dag_id = 'discourse_crawl_topic_content',
    schedule_interval=None,
    start_date=datetime(2022, 9, 24),
    catchup=False,
    tags=['discourse']
) as dag:

    def scheduler_init_crawl_discourse_topic_content(ds, **kwargs):
        return 'End::scheduler_init_crawl_discourse_topic_content'


    op_scheduler_init_crawl_discourse_topic_content = PythonOperator(
        task_id='scheduler_init_crawl_discourse_topic_content',
        python_callable=scheduler_init_crawl_discourse_topic_content
    )

    def do_crawl_discourse_topic_content(params):
        from airflow.models import Variable
        from oss_know.libs.discourse import crawl_topic_content
        owner = params["owner"]
        repo  = params["repo"]
        url   = params["url"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        crawl_discourse_topic_content_info = crawl_topic_content.crawl_topic_content(base_url=url,
                                                                            owner=owner,
                                                                            repo=repo,
                                                                            opensearch_conn_datas=opensearch_conn_datas)
        return 'do_crawl_discourse_topic_content:::end'

    from airflow.models import Variable

    discourse_info_list = Variable.get(NEED_INIT_DISCOURSE, deserialize_json=True)
    for discourse_info in discourse_info_list:
        op_do_crawl_discourse_topic_content = PythonOperator(
            task_id=f'do_crawl_discourse_topic_content_{discourse_info["owner"]}_{discourse_info["repo"]}',
            python_callable=do_crawl_discourse_topic_content,
            op_kwargs={'params': discourse_info}
        )

        op_scheduler_init_crawl_discourse_topic_content >>op_do_crawl_discourse_topic_content
