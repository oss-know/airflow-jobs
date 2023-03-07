from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CLICKHOUSE_DRIVER_INFO, \
    CK_TABLE_DEFAULT_VAL_TPLT

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

    from airflow.models import Variable


    def do_ck_transfer_data_by_repo(params, owner_repo_or_project_maillist_name):
        from oss_know.libs.clickhouse import init_ck_transfer_data
        opensearch_index = params["index"]
        table_name = params["index"]
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)

        if table_name.startswith("github_issues_timeline"):
            init_ck_transfer_data.transfer_data_special_by_repo(
                clickhouse_server_info=clickhouse_server_info,
                opensearch_index=opensearch_index,
                table_name=table_name,
                opensearch_conn_datas=opensearch_conn_datas, search_key=owner_repo_or_project_maillist_name)
        else:
            table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
            table_template = table_templates.get(table_name)
            if table_name.startswith("maillist"):
                init_ck_transfer_data.transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=opensearch_index,
                    table_name=table_name,
                    opensearch_conn_datas=opensearch_conn_datas,
                    template=table_template,
                    owner_repo_or_project_maillist_name=owner_repo_or_project_maillist_name,
                    transfer_type='maillist_init')
            else:
                init_ck_transfer_data.transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=opensearch_index,
                    table_name=table_name,
                    opensearch_conn_datas=opensearch_conn_datas,
                    template=table_template,
                    owner_repo_or_project_maillist_name=owner_repo_or_project_maillist_name,
                    transfer_type='github_git_init_by_repo')

        return 'do_ck_transfer_data_by_repo:::end'


    need_init_transfer_to_clickhouse = Variable.get("need_init_transfer_to_clickhouse", deserialize_json=True)

    for index_and_repo in need_init_transfer_to_clickhouse:
        if index_and_repo["index"].startswith('maillist'):
            for owner_repo_or_project_maillist_name in index_and_repo["repo_list"]:
                op_do_ck_transfer_data_by_repo = PythonOperator(
                    task_id=f'do_ck_transfer_os_index_{index_and_repo["index"]}_project_name_{owner_repo_or_project_maillist_name.get("project_name")}_mail_list_name_{owner_repo_or_project_maillist_name.get("mail_list_name")}',
                    python_callable=do_ck_transfer_data_by_repo,
                    op_kwargs={'params': index_and_repo,
                               'owner_repo_or_project_maillist_name': owner_repo_or_project_maillist_name},
                )
                op_init_clickhouse_transfer_data_by_repo >> op_do_ck_transfer_data_by_repo
        else:
            for owner_repo_or_project_maillist_name in index_and_repo["repo_list"]:
                op_do_ck_transfer_data_by_repo = PythonOperator(
                    task_id=f'do_ck_transfer_os_index_{index_and_repo["index"]}_owner_{owner_repo_or_project_maillist_name.get("owner")}_repo_{owner_repo_or_project_maillist_name.get("repo")}',
                    python_callable=do_ck_transfer_data_by_repo,
                    op_kwargs={'params': index_and_repo,
                               'owner_repo_or_project_maillist_name': owner_repo_or_project_maillist_name},
                )

                op_init_clickhouse_transfer_data_by_repo >> op_do_ck_transfer_data_by_repo
