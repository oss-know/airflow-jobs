from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# statistics_metrics_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_CREATE_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO

with DAG(
        dag_id='ck_statistics_metrics',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['analysis'],
) as dag:
    def init_statistics_metrics(ds, **kwargs):
        return 'Start init_statistics_metrics'


    op_init_statistics_metrics = PythonOperator(
        task_id='init_statistics_metrics',
        python_callable=init_statistics_metrics,
    )

    def do_statistics_metrics(params):
        from airflow.models import Variable
        from oss_know.libs.metrics import init_statistics_metrics
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        init_statistics_metrics.statistics_metrics(clickhouse_server_info=clickhouse_server_info)
        return "end::do_statistics_metrics"


    op_do_statistics_metrics = PythonOperator(
        task_id=f'do_statistics_metrics',
        python_callable=do_statistics_metrics
    )

    op_init_statistics_metrics >> op_do_statistics_metrics
