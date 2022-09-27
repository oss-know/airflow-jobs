from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch, helpers

from oss_know.libs.base_dict.variable_key import NEED_INIT_DISCOURSE

with DAG(
    dag_id = 'discourse_crawl_category',
    schedule_interval=None,
    start_date=datetime(2022, 9, 24),
    catchup=False,
    tags=['discourse']
) as dag:
    def init_crawl_discourse_category(ds, **kwargs):
        return 'Start init_crawl_discourse_category'

    op_init_crawl_discourse_category = PythonOperator(
        task_id='init_crawl_discourse_category',
        python_callable=init_crawl_discourse_category
    )

    def do_crawl_discourse_category(params):
        from airflow.models import Variable
        from oss_know.libs.discourse import crawl_category
        owner = params["owner"]
        repo  = params["repo"]
        url   = params["url"]
        proxy_config = params.get("proxy_config")
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)

        crawl_discourse_category_info = crawl_category.crawl_discourse_category(base_url=url,
                                                                                owner=owner,
                                                                                repo=repo,
                                                                                proxy_config=proxy_config,
                                                                                opensearch_conn_datas=opensearch_conn_datas)

        return 'do_crawl_discourse_category:::end'

    from airflow.models import Variable

    discourse_info_list = Variable.get(NEED_INIT_DISCOURSE, deserialize_json=True)
    for discourse_info in discourse_info_list:
        op_do_crawl_discourse_category = PythonOperator(
            task_id=f'do_crawl_discourse_category_{discourse_info["owner"]}_{discourse_info["repo"]}',
            python_callable=do_crawl_discourse_category,
            op_kwargs={'params': discourse_info}
        )

        op_init_crawl_discourse_category >>op_do_crawl_discourse_category
