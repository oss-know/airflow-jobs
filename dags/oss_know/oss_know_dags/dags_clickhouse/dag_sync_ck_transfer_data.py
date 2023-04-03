from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_CREATE_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO, \
    CK_TABLE_MAP_FROM_OS_INDEX, OPENSEARCH_CONN_DATA

with DAG(
        dag_id='sync_ck_transfer_data',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def init_clickhouse_transfer_data(ds, **kwargs):
        return 'Start init_clickhouse_transfer_data'


    op_init_clickhouse_transfer_data = PythonOperator(
        task_id='init_clickhouse_transfer_data',
        python_callable=init_clickhouse_transfer_data,
    )


    def do_sync_ck_transfer_data(params):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import sync_ck_transfer_data
        from oss_know.libs.clickhouse import sync_ck_transfer_data
        opensearch_index = params["OPENSEARCH_INDEX"]
        table_name = params["CK_TABLE_NAME"]
        opensearch_conn_datas = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

        if table_name == 'github_issues_timeline':
            transfer_data = sync_ck_transfer_data.sync_transfer_data_spacial(
                clickhouse_server_info=clickhouse_server_info,
                opensearch_index=opensearch_index,
                table_name=table_name,
                opensearch_conn_datas=opensearch_conn_datas)
        else:
            transfer_data = sync_ck_transfer_data.sync_transfer_data(clickhouse_server_info=clickhouse_server_info,
                                                                     opensearch_index=opensearch_index,
                                                                     table_name=table_name,
                                                                     opensearch_conn_datas=opensearch_conn_datas)
        return 'do_ck_sync_transfer_data:::end'


    from airflow.models import Variable

    os_index_ck_tb_infos = Variable.get(CK_TABLE_MAP_FROM_OS_INDEX, deserialize_json=True)
    for os_index_ck_tb_info in os_index_ck_tb_infos:
        op_do_ck_transfer_data = PythonOperator(
            task_id=f'do_ck_sync_transfer_data_os_index_{os_index_ck_tb_info["OPENSEARCH_INDEX"]}_ck_tb_'
                    f'{os_index_ck_tb_info["CK_TABLE_NAME"]}',
            python_callable=do_sync_ck_transfer_data,
            op_kwargs={'params': os_index_ck_tb_info},
        )

        op_init_clickhouse_transfer_data >> op_do_ck_transfer_data
