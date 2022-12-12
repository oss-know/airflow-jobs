import json
import calendar
import pandas as pd
from datetime import datetime
from loguru import logger
from airflow import DAG
from airflow.operators.python import PythonOperator
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO
from oss_know.libs.gh_archive import gh_archive
gha = gh_archive.GHArchive()


results = gha.get_hour(current_date=datetime.datetime(2022, 11, 27, 19))
with DAG(
        dag_id='sync_gha_transfer2ck.',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['gh_archive'],
        concurrency=6
) as dag:
    def sync_clickhouse_transfer_data(ds, **kwargs):
        return 'Start sync_gha_transfer2ck'


    op_sync_gha_transfer2ck = PythonOperator(
        task_id='init_clickhouse_transfer_data_by_repo',
        python_callable=sync_clickhouse_transfer_data,
    )


    def do_sync_transfer_gha_2ck(year_month_day_list):

        for i in year_month_day_list:
            url = f"https://data.gharchive.org/{i}.json.gz"
            print(url)

        return "end do_transfer_gha_2ck"

    for result in results:
        date_array = result.split('-')
        year = date_array[0]
        month = date_array[1]
        day = date_array[2]
        op_do_sync_transfer_gha_2ck = PythonOperator(
            task_id=f'do_transfer_gha_2ck_year_{year}_month_{month}_day_{day}',
            python_callable=do_sync_transfer_gha_2ck,
            op_kwargs={'year_month_day_list': results[result]},
        )



