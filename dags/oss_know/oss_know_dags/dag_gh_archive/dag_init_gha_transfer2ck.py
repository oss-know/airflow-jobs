import json
import calendar
import pandas as pd
from datetime import datetime
from loguru import logger
from airflow import DAG
from airflow.operators.python import PythonOperator
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO


with DAG(
        dag_id='init_gha_transfer2ck.',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['gh_archive'],
        concurrency=6
) as dag:
    def init_clickhouse_transfer_data(ds, **kwargs):
        return 'Start init_gha_transfer2ck'


    op_init_gha_transfer2ck = PythonOperator(
        task_id='init_clickhouse_transfer_data_by_repo',
        python_callable=init_clickhouse_transfer_data,
    )


    def do_transfer_gha_2ck(year, month, day):
        from airflow.models import Variable
        from oss_know.libs.gha.transfer_data_to_ck import parse_json_data
        logger.info(f"year:{year}_month:{month}_day:{day}")
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        parse_json_data(year, month, day, clickhouse_server_info=clickhouse_server_info)
        return "end do_transfer_gha_2ck"

    for year in [2022]:


        for month in range(9, 10):

            day_count = calendar.monthrange(year, month)[1]
            if month < 10:
                month = '0' + str(month)
            # for i in range(1, day_count + 1):
            for i in range(1, 2):
                if i < 10:
                    i = '0' + str(i)
                op_do_transfer_gha_2ck = PythonOperator(
                    task_id=f'do_transfer_gha_2ck_year_{year}_month_{month}_day_{i}',
                    python_callable=do_transfer_gha_2ck,
                    op_kwargs={'year': year, "month": month, 'day': i},
                )
                op_init_gha_transfer2ck >> op_do_transfer_gha_2ck
