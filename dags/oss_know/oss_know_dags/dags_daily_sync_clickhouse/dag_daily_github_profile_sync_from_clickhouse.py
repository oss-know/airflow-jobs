from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, SYNC_FROM_CLICKHOUSE_DRIVER_INFO, \
    CLICKHOUSE_SYNC_INTERVAL, CLICKHOUSE_SYNC_COMBINATION_TYPE
from oss_know.libs.clickhouse.sync_clickhouse_data import sync_github_profiles_from_remote_ck

clickhouse_conn_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
sync_from_clickhouse_conn_info = Variable.get(SYNC_FROM_CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
sync_interval = Variable.get(CLICKHOUSE_SYNC_INTERVAL, default_var=None)
sync_combination_type = Variable.get(CLICKHOUSE_SYNC_COMBINATION_TYPE, default_var="union")

# Daily sync github_profile data from other clickhouse environment
with DAG(dag_id='daily_github_profile_sync_from_clickhouse',  # schedule_interval='*/5 * * * *',
         schedule_interval=sync_interval, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync clickhouse'], ) as dag:
    def do_sync_github_profile_from_clickhouse():
        sync_github_profiles_from_remote_ck(clickhouse_conn_info, sync_from_clickhouse_conn_info)


    op_sync_github_profile_from_clickhouse_group = PythonOperator(
        task_id=f'op_sync_github_profile_from_clickhouse',
        python_callable=do_sync_github_profile_from_clickhouse,
    )
