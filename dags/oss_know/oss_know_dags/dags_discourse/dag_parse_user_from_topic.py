from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch, helpers

from oss_know.libs.base_dict.variable_key import NEED_INIT_DISCOURSE


with DAG(
    dag_id = 'discourse_parse_user_from_topic',
    schedule_interval=None,
    start_date=datetime(2022, 9, 26),
    catchup=False,
    tags=['discourse']
) as dag:

    def scheduler_init_parse_user_from_topic(ds, **kwargs):
        return 'End::scheduler_init_parse_user_from_topic'


    op_scheduler_init_parse_user_from_topic = PythonOperator(
        task_id='scheduler_init_parse_user_from_topic',
        python_callable=scheduler_init_parse_user_from_topic
    )

    def do_parse_user_from_topic(params):
        from airflow.models import Variable
        from oss_know.libs.discourse import parse_user_from_topic
        owner = params["owner"]
        repo  = params["repo"]
        url   = params["url"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        parse_user_from_topic_info = parse_user_from_topic.parse_user_from_topic(base_url=url,
                                                                            owner=owner,
                                                                            repo=repo,
                                                                            opensearch_conn_datas=opensearch_conn_datas)
        return 'do_parse_user_from_topic:::end'

    from airflow.models import Variable

    discourse_info_list = Variable.get(NEED_INIT_DISCOURSE, deserialize_json=True)
    for discourse_info in discourse_info_list:
        op_do_parse_user_from_topic = PythonOperator(
            task_id=f'do_parse_user_from_topic_{discourse_info["owner"]}_{discourse_info["repo"]}',
            python_callable=do_parse_user_from_topic,
            op_kwargs={'params': discourse_info}
        )

        op_scheduler_init_parse_user_from_topic >>op_do_parse_user_from_topic
