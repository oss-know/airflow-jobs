import json
import calendar

import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_CREATE_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO, \
    CK_TABLE_MAP_FROM_OS_INDEX, CK_TABLE_DEFAULT_VAL_TPLT, REPO_LIST, MAILLIST_REPO
from oss_know.libs.gha.transfer_data_to_ck import transfer_data_by_repo, parse_json_data

with DAG(
        dag_id='init_gha_transfer2ck.',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['gh_archive'],
        concurrency=18
) as dag:
    def init_clickhouse_transfer_data(ds, **kwargs):
        return 'Start init_gha_transfer2ck'


    op_init_gha_transfer2ck = PythonOperator(
        task_id='init_clickhouse_transfer_data_by_repo',
        python_callable=init_clickhouse_transfer_data,
    )


    def do_transfer_gha_2ck(year, month, day):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import init_ck_transfer_data
        print(year)
        print(month)
        print(day)
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        parse_json_data(year, month, day, clickhouse_server_info=clickhouse_server_info,
                        opensearch_conn_datas=opensearch_conn_datas)
        return "end do_transfer_gha_2ck"


    from airflow.models import Variable

    for year in range(2019, 2020):

        for month in range(12, 13):
            day_count = calendar.monthrange(year, month)[1]
            if month < 10:
                month = '0' + str(month)
            for i in range(1, 2):
                if i < 10:
                    i = '0' + str(i)
                op_do_transfer_gha_2ck = PythonOperator(
                    task_id=f'do_transfer_gha_2ck_year_{year}_month_{month}_day_{i}',
                    python_callable=do_transfer_gha_2ck,
                    op_kwargs={'year': year, "month": month, 'day': i},
                )
                op_init_gha_transfer2ck >> op_do_transfer_gha_2ck

    # for index_and_repo in need_init_transfer_to_clickhouse:
    #     if index_and_repo["index"].startswith('maillist'):
    #         for repo in index_and_repo["repo_list"]:
    #             op_do_transfer_gha_2ck = PythonOperator(
    #                             task_id=f'do_ck_transfer_os_index_{index_and_repo["index"]}_project_name_{repo.get("project_name")}_mail_list_name_{repo.get("mail_list_name")}',
    #                             python_callable=do_transfer_gha_2ck,
    #                             op_kwargs={'params': index_and_repo, 'search_key': repo},
    #                         )
    #             op_init_gha_transfer2ck >> op_do_transfer_gha_2ck
