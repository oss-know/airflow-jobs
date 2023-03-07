from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_ALTER_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO

with DAG(
        dag_id='ck_alter_tables',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def init_clickhouse_ddl(ds, **kwargs):
        return 'Start init_clickhouse_ddl'


    op_init_clickhouse_ddl = PythonOperator(
        task_id='init_clickhouse_ddl',
        python_callable=init_clickhouse_ddl,
    )

    from airflow.models import Variable


    def do_ck_alter_table(params):
        from oss_know.libs.clickhouse import ck_alter_table
        table_name = params["table_name"]
        cluster_name = params["cluster_name"]
        parse_data = params.get("parse_data")
        database_name = params.get("database_name")
        distributed_key = params.get("distributed_key")
        df = pd.json_normalize(parse_data)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        ck_alter_table.create_ck_table(df=df,
                                       database_name=database_name,
                                       distributed_key=distributed_key,
                                       cluster_name=cluster_name,
                                       table_name=table_name,
                                       clickhouse_server_info=clickhouse_server_info)
        return 'do_ck_create_table:::end'


    ck_table_infos = Variable.get(CK_ALTER_TABLE_COLS_DATATYPE_TPLT, deserialize_json=True)
    for table_info in ck_table_infos:
        op_do_ck_create_table = PythonOperator(
            task_id=f'do_ck_alter_table_table_name_{table_info["table_name"]}',
            python_callable=do_ck_alter_table,
            op_kwargs={'params': table_info},
        )

        op_init_clickhouse_ddl >> op_do_ck_create_table
