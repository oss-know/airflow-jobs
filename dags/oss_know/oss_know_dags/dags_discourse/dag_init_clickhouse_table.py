import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_ALTER_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.base_dict.variable_key import NEED_INIT_DISCOURSE_CLICKHOUSE

with DAG(
        dag_id='create_discourse_table_in_clickhouse',
        schedule_interval=None,
        start_date=datetime(2022, 9, 30),
        catchup=False,
        tags=['discourse'],
) as dag:
    def init_create_discourse_table_in_clickhouse(ds, **kwargs):
        return 'Start init_create_discourse_table_in_clickhouse'

    
    op_init_create_discourse_table_in_clickhouse = PythonOperator(
        task_id='init_create_discourse_table_in_clickhouse',
        python_callable=init_create_discourse_table_in_clickhouse,
    )
    
    # copy from do_ck_create_table
    def create_discourse_table_in_clickhouse(params):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import ck_create_table
        table_name = params["table_name"]
        cluster_name = params["cluster_name"]
        table_engine = params["table_engine"]
        partition_by = params["partition_by"]
        order_by = params.get("order_by")
        parse_data = params.get("parse_data")
        database_name = params.get("database_name")
        distributed_key = params.get("distributed_key")
        df = pd.json_normalize(parse_data)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        create_table = ck_create_table.create_ck_table(df=df,
                                                       database_name=database_name,
                                                       distributed_key=distributed_key,
                                                       cluster_name=cluster_name,
                                                       table_name=table_name,
                                                       table_engine=table_engine,
                                                       partition_by=partition_by,
                                                       order_by=order_by,
                                                       clickhouse_server_info=clickhouse_server_info)
        return 'create_discourse_table_in_clickhouse:::end'


    from airflow.models import Variable

    ck_table_infos = Variable.get(NEED_INIT_DISCOURSE_CLICKHOUSE, deserialize_json=True)
    for table_info in ck_table_infos:
        op_create_discourse_table_in_clickhouse = PythonOperator(
            task_id=f'do_create_discourse_table_in_clickhouse_table_name_{table_info["table_name"]}',
            python_callable=create_discourse_table_in_clickhouse,
            op_kwargs={'params': table_info},
        )

        op_init_create_discourse_table_in_clickhouse >> op_create_discourse_table_in_clickhouse
