import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from oss_know.libs.util.log import logger
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_CREATE_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.util.clickhouse_driver import CKServer

with DAG(
        dag_id='create_table_use_ddl',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def init_clickhouse_ddl(ds, **kwargs):
        return 'Start init_clickhouse_ddl'


    op_init_clickhouse_ddl = PythonOperator(
        task_id='clickhouse_ddl_init',
        python_callable=init_clickhouse_ddl,
    )

#     "ck_table_infos": [
#         {
#             "local_table": """
#             create table github_id_main_tz_map_local on cluster replicated
# (
#     update_at           DateTime64(3),
#     update_at_timestamp Int64,
#     github_id           Int64,
#     main_tz_area        String,
#     top_n_tz_area Array(Tuple(Array(String),String,Int64))
# )
#     engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/github_id_main_tz_map', '{replica}')
#         ORDER BY (github_id)
#         SETTINGS index_granularity = 8192;
#             """,
#             "distributed_table": """
#             create table github_id_main_tz_map on cluster replicated as github_id_main_tz_map_local
# engine = Distributed('replicated', 'default', 'github_id_main_tz_map_local', update_at_timestamp);
#             """
#         }
#     ]
    def do_ck_create_table(params):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import ck_create_table
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        local_table = params['local_table']
        distributed_table = params['distributed_table']
        ck = CKServer(host=clickhouse_server_info["HOST"],
                      port=clickhouse_server_info["PORT"],
                      user=clickhouse_server_info["USER"],
                      password=clickhouse_server_info["PASSWD"],
                      database=clickhouse_server_info["DATABASE"])
        create_local_table_resp = ck.execute_no_params(local_table)
        logger.info(create_local_table_resp)
        create_distributed_table_resp = ck.execute_no_params(distributed_table)
        logger.info(create_distributed_table_resp)

        return 'do_ck_create_table:::end'


    from airflow.models import Variable

    ck_table_infos = Variable.get("ck_create_table_ddl", deserialize_json=True)
    for table_info in ck_table_infos:
        op_do_ck_create_table = PythonOperator(
            task_id=f'ck_create_table_use_ddl_{table_info["table_name"]}',
            python_callable=do_ck_create_table,
            op_kwargs={'params': table_info},
        )

        op_init_clickhouse_ddl >> op_do_ck_create_table
