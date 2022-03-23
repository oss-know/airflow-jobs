import json

import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_CREATE_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO, \
    CK_TABLE_MAP_FROM_OS_INDEX, CK_TABLE_DEFAULT_VAL_TPLT

with DAG(
        dag_id='init_ck_transfer_data_by_repo',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['clickhouse'],
) as dag:
    def init_clickhouse_transfer_data(ds, **kwargs):
        return 'Start init_clickhouse_transfer_data_by_repo'


    op_init_clickhouse_transfer_data_by_repo = PythonOperator(
        task_id='init_clickhouse_transfer_data_by_repo',
        python_callable=init_clickhouse_transfer_data,
    )


    def do_ck_transfer_data_by_repo(params, search_key):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import init_ck_transfer_data
        opensearch_index = params["OPENSEARCH_INDEX"]
        table_name = params["CK_TABLE_NAME"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        template = {}

        # print(json.dumps(temp))
        # raise Exception("我想看看初始化的结果")
        if table_name.startswith("github_issues_timeline"):
            transfer_data = init_ck_transfer_data.transfer_data_special_by_repo(clickhouse_server_info=clickhouse_server_info,
                                                                        opensearch_index=opensearch_index,
                                                                        table_name=table_name,
                                                                        opensearch_conn_datas=opensearch_conn_datas, search_key=search_key)
        else:
            table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
            for table_template in table_templates:
                if table_template.get("table_name") == table_name:
                    template = table_template.get("temp")
                    break
            df = pd.json_normalize(template)
            template = init_ck_transfer_data.parse_data_init(df)
            if table_name.startswith("maillist"):
                transfer_data = init_ck_transfer_data.transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=opensearch_index,
                    table_name=table_name,
                    opensearch_conn_datas=opensearch_conn_datas,
                    template=template, search_key=search_key, type='maillist_init')
            else:
                transfer_data = init_ck_transfer_data.transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=opensearch_index,
                    table_name=table_name,
                    opensearch_conn_datas=opensearch_conn_datas,
                    template=template, search_key=search_key,type='github_git_init_by_repo')

        return 'do_ck_transfer_data_by_repo:::end'


    from airflow.models import Variable

    os_index_ck_tb_infos = Variable.get(CK_TABLE_MAP_FROM_OS_INDEX, deserialize_json=True)
    owner_repo_list = Variable.get("repo_list", deserialize_json=True)
    maillist_repo_list = Variable.get("maillist_repo", deserialize_json=True)
    for os_index_ck_tb_info in os_index_ck_tb_infos:
        if os_index_ck_tb_info["OPENSEARCH_INDEX"].startswith('maillist'):
            for maillist_repo in maillist_repo_list:
                op_do_ck_transfer_data_by_repo = PythonOperator(
                    task_id=f'do_ck_transfer_os_index_{os_index_ck_tb_info["OPENSEARCH_INDEX"]}_ck_tb_{os_index_ck_tb_info["CK_TABLE_NAME"]}_project_name_{maillist_repo.get("project_name")}_mail_list_name_{maillist_repo.get("mail_list_name")}',
                    python_callable=do_ck_transfer_data_by_repo,
                    op_kwargs={'params': os_index_ck_tb_info, 'search_key': maillist_repo},
                )

                op_init_clickhouse_transfer_data_by_repo >> op_do_ck_transfer_data_by_repo
        else:
            for owner_repo in owner_repo_list:
                op_do_ck_transfer_data_by_repo = PythonOperator(
                    task_id=f'do_ck_transfer_os_index_{os_index_ck_tb_info["OPENSEARCH_INDEX"]}_ck_tb_{os_index_ck_tb_info["CK_TABLE_NAME"]}_owner_{owner_repo.get("owner")}_repo_{owner_repo.get("repo")}',
                    python_callable=do_ck_transfer_data_by_repo,
                    op_kwargs={'params': os_index_ck_tb_info, 'search_key': owner_repo},
                )

                op_init_clickhouse_transfer_data_by_repo >> op_do_ck_transfer_data_by_repo
