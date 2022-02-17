from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pandas import json_normalize
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import NEED_CK_TABLE_INFOS, CLICKHOUSE_DRIVER_INFO

with DAG(
        dag_id='ck_create_tables',
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

    # "ck_table_infos": [
    #     {
    #         "table_name": "table1",
    #         "table_engine": "MergeTree"
    #         "partition_by":""
    #         "order_by":""
    #         "parse_data":
    #     }
    # ]
    def do_ck_create_table(params):
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
        df = json_normalize(parse_data)
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
        return 'do_ck_create_table:::end'


    from airflow.models import Variable

    ck_table_infos = Variable.get(NEED_CK_TABLE_INFOS, deserialize_json=True)
    for table_info in ck_table_infos:
        op_do_ck_create_table = PythonOperator(
            task_id=f'do_ck_create_table_table_name_{table_info["table_name"]}',
            python_callable=do_ck_create_table,
            op_kwargs={'params': table_info},
        )

        op_init_clickhouse_ddl >> op_do_ck_create_table
