from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, \
    CK_TABLE_DEFAULT_VAL_TPLT, OPENSEARCH_CONN_DATA, CK_TABLE_SYNC_MAP_FROM_OS_INDEX, \
    SYNC_REPO_LIST
from oss_know.libs.util.data_transfer import parse_data_init

with DAG(
        dag_id='sync_ck_transfer_data_by_repo',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def init_sync_clickhouse_transfer_data(ds, **kwargs):
        return 'Start init_sync_clickhouse_transfer_data'


    op_init_clickhouse_transfer_data_by_repo = PythonOperator(
        task_id='init_clickhouse_transfer_data_by_repo',
        python_callable=init_sync_clickhouse_transfer_data,
    )


    def do_sync_ck_transfer_data_by_repo(params, owner_repo):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import sync_ck_transfer_data
        opensearch_index = params["OPENSEARCH_INDEX"]
        table_name = params["CK_TABLE_NAME"]
        opensearch_conn_datas = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        template = {}
        # TODO Restructure the code if needed, or delete it if it's no longer used
        if table_name.startswith("github_issues_timeline"):
            transfer_data = sync_ck_transfer_data.sync_transfer_data_spacial_by_repo(
                clickhouse_server_info=clickhouse_server_info,
                opensearch_index=opensearch_index,
                table_name=table_name,
                opensearch_conn_datas=opensearch_conn_datas, owner_repo=owner_repo)
        else:
            table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
            for table_template in table_templates:
                if table_template.get("table_name") == table_name:
                    template = table_template.get("temp")
                    break
            df = pd.json_normalize(template)
            template = parse_data_init(df)
            if table_name.startswith("maillist"):
                transfer_data = sync_ck_transfer_data.sync_transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=opensearch_index,
                    table_name=table_name,
                    opensearch_conn_datas=opensearch_conn_datas,
                    template=template, owner_repo=owner_repo, transfer_type="maillist_init")
            else:

                transfer_data = sync_ck_transfer_data.sync_transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=opensearch_index,
                    table_name=table_name,
                    opensearch_conn_datas=opensearch_conn_datas,
                    template=template, owner_repo=owner_repo, transfer_type="github_git_sync_by_repo")
        return 'do_sync_ck_transfer_data_by_repo:::end'


    from airflow.models import Variable

    os_index_ck_tb_infos = Variable.get(CK_TABLE_SYNC_MAP_FROM_OS_INDEX, deserialize_json=True)
    owner_repo_list = Variable.get(SYNC_REPO_LIST, deserialize_json=True)
    for os_index_ck_tb_info in os_index_ck_tb_infos:
        for owner_repo in owner_repo_list:
            op_do_sync_ck_transfer_data_by_repo = PythonOperator(
                task_id=f'do_ck_transfer_os_index_{os_index_ck_tb_info["OPENSEARCH_INDEX"]}_ck_tb_'
                        f'{os_index_ck_tb_info["CK_TABLE_NAME"]}_owner_{owner_repo.get("owner"
            )}_repo_{owner_repo.get("repo")}',
                python_callable=do_sync_ck_transfer_data_by_repo,
                op_kwargs={'params': os_index_ck_tb_info, 'owner_repo': owner_repo},
            )

            op_init_clickhouse_transfer_data_by_repo >> op_do_sync_ck_transfer_data_by_repo
