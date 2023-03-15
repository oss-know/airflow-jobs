from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch, helpers

from oss_know.libs.base_dict.variable_key import ZULIP_API_KEYS, NEED_INIT_ZULIP, OPENSEARCH_CONN_DATA

with DAG(
    dag_id = 'zulip_crawl_stream',
    schedule_interval=None,
    start_date=datetime(2023, 3, 1),
    catchup=False,
    tags=['zulip']
) as dag:
    def init_zulip_crawl_stream(ds, **kwargs):
        return 'Start init_zulip_crawl_stream'
    
    op_init_zulip_crawl_stream = PythonOperator(
        task_id='init_zulip_crawl_stream',
        python_callable=init_zulip_crawl_stream
    )

    def do_zulip_crawl_stream(params):
        from airflow.models import Variable
        from oss_know.libs.zulip import crawl_stream
        from oss_know.libs.util.zulip import get_api
        owner = params["owner"]
        repo = params["repo"]
        site = params["site"]
        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        api_key = Variable.get(ZULIP_API_KEYS, deserialize_json=True)

        client = get_api(email=api_key["email"], api_key=api_key["api_key"], site=site)
        crawl_zulip_stream_info = crawl_stream.crawl_zulip_stream(client=client,
                                                                  owner=owner,
                                                                  repo=repo,
                                                                  site=site,
                                                                  opensearch_conn_info=opensearch_conn_info)

        return 'do_zulip_crawl_stream:::end'

    from airflow.models import Variable

    zulip_info_list = Variable.get(NEED_INIT_ZULIP, deserialize_json=True)
    for zulip_info in zulip_info_list:
        op_do_zulip_crawl_stream = PythonOperator(
            task_id=f'do_zulip_crawl_stream_{zulip_info["owner"]}_{zulip_info["repo"]}',
            python_callable=do_zulip_crawl_stream,
            op_kwargs={'params': zulip_info}
        )

        op_init_zulip_crawl_stream >>op_do_zulip_crawl_stream
