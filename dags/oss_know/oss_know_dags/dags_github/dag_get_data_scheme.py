from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA, GITHUB_TOKENS, REDIS_CLIENT_DATA, \
    DURATION_OF_SYNC_GITHUB_PROFILES,SYNC_PROFILES_TASK_NUM
from airflow.models import Variable

# v0.0.1

with DAG(
        dag_id='github_data0101',
        schedule_interval=None,
        start_date=datetime(2000, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def data_scheme(ds, **kwargs):
        return 'End:data'


    op_scheduler_get_data_scheme = PythonOperator(
        task_id='op_data',
        python_callable=data_scheme
    )

    def get_data_scheme():
        from oss_know.libs.github import get_data_scheme

        opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)

        get_data_scheme.get_data_scheme(opensearch_conn_info=opensearch_conn_info)


    op_get_data_scheme = PythonOperator(
        task_id='op_get_data_scheme',
        python_callable=get_data_scheme,
        provide_context=True,
    )

    op_scheduler_get_data_scheme >> op_get_data_scheme
