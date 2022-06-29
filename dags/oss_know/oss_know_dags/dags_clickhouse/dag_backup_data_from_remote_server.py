# -*-coding:utf-8-*-
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_ALTER_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO


with DAG(
        dag_id='backup_data_from_remote_server',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def init_backup_data_from_remote_server(ds, **kwargs):
        return 'Start init_backup_data_from_remote_server'


    op_init_backup_data_from_remote_server = PythonOperator(
        task_id='init_backup_data_from_remote_server',
        python_callable=init_backup_data_from_remote_server,
    )

    def do_backup_data_from_remote_server(params):
        from oss_know.libs.clickhouse.remote_back_up import back_up_from_remote_server

        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        back_up_from_remote_server(clickhouse_server_info=clickhouse_server_info,params=params)


    from airflow.models import Variable
    # NEED_CK_TABLE_INFOS改成NEED_ALTER_CK_TABLE_INFOS
    dict1 = [

        {"remote_address":"",
         "remote_table_name":"",
         "local_table_name":"",
         "remote_user":"",
         "remote_password":""}

    ]
    # ck_table_infos = Variable.get(CK_ALTER_TABLE_COLS_DATATYPE_TPLT, deserialize_json=True)
    for dict in dict1:
        op_do_backup_data_from_remote_server = PythonOperator(
            task_id=f'do_backup_data_from_remote_server{dict["table_name"]}',
            python_callable=do_backup_data_from_remote_server,
            op_kwargs={'params': dict},
        )

        op_init_backup_data_from_remote_server >> op_do_backup_data_from_remote_server
