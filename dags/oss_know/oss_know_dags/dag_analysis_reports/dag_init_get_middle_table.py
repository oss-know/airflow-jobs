# -*-coding:utf-8-*-
import random
import time
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.analysis_report.email_tz_region_map import get_email_tz_region_map
from oss_know.libs.analysis_report.github_id_email_map import get_github_id_email_map
from oss_know.libs.analysis_report.github_id_tz_region_map import get_github_id_tz_region_map
from oss_know.libs.analysis_report.timeline_approved_reviewed import get_github_id_approved_reviewed
# git_init_sync_v0.0.3
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.util.log import logger

with DAG(
        dag_id='dag_init_get_middle_table',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['analysis'],
) as dag:
    def init_get_middle_table(ds, **kwargs):
        return 'Start init_get_middle_table'


    op_init_get_middle_table = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_get_middle_table,
    )


    def do_get_email_tz_region_map():
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        wait_time = random.randint(1, 60)
        logger.info(f"等待{wait_time} s ...")
        time.sleep(wait_time)
        get_email_tz_region_map(clickhouse_server_info)
        return 'do_get_email_tz_region_map:::end'


    def do_get_github_id_tz_region_map():
        wait_time = random.randint(1, 60)
        logger.info(f"等待{wait_time} s ...")
        time.sleep(wait_time)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_github_id_tz_region_map(clickhouse_server_info)


    def do_get_github_id_approved_reviewed():
        wait_time = random.randint(1, 60)
        logger.info(f"等待{wait_time} s ...")
        time.sleep(wait_time)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_github_id_approved_reviewed(clickhouse_server_info)


    def do_get_github_id_email_map():
        wait_time = random.randint(1, 60)
        logger.info(f"等待{wait_time} s ...")
        time.sleep(wait_time)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        get_github_id_email_map(clickhouse_server_info)


    op_do_get_email_tz_region_map = PythonOperator(
        task_id=f'do_get_email_tz_region_map',
        python_callable=do_get_email_tz_region_map
    )
    op_do_get_github_id_tz_region_map = PythonOperator(
        task_id=f'do_get_github_id_tz_region_map',
        python_callable=do_get_github_id_tz_region_map
    )
    op_do_get_github_id_approved_reviewed = PythonOperator(
        task_id=f'do_get_github_id_approved_reviewed',
        python_callable=do_get_github_id_approved_reviewed
    )
    op_do_get_github_id_email_map = PythonOperator(
        task_id=f'do_get_github_id_email_map',
        python_callable=do_get_github_id_email_map
    )

    op_init_get_middle_table >> op_do_get_email_tz_region_map
    op_init_get_middle_table >> op_do_get_github_id_tz_region_map
    op_init_get_middle_table >> op_do_get_github_id_approved_reviewed
    op_init_get_middle_table >> op_do_get_github_id_email_map
