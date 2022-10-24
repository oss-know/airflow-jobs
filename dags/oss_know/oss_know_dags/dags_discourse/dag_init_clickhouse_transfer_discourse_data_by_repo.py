import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# clickhouse_init_sync_v0.0.1
from oss_know.libs.base_dict.variable_key import CK_ALTER_TABLE_COLS_DATATYPE_TPLT, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.base_dict.variable_key import NEED_INIT_DISCOURSE_CLICKHOUSE
from oss_know.libs.base_dict.variable_key import CK_TABLE_DEFAULT_VAL_TPLT
with DAG(
        dag_id='transfer_discourse_data_to_clickhouse',
        schedule_interval=None,
        start_date=datetime(2022, 9, 30),
        catchup=False,
        tags=['discourse'],
) as dag:
    def init_transfer_discourse_data_to_clickhouse(ds, **kwargs):
        return 'Start init_transfer_discourse_data_to_clickhouse'


    op_init_transfer_discourse_data_to_clickhouse = PythonOperator(
        task_id='init_transfer_discourse_data_to_clickhouse',
        python_callable=init_transfer_discourse_data_to_clickhouse,
    )

    # Copy from do_ck_transfer_data_by_repo.
    def transfer_discourse_data_to_clickhouse(params, search_key):
        from airflow.models import Variable
        from oss_know.libs.clickhouse import init_ck_transfer_data

        opensearch_index = params["index"]
        if opensearch_index == 'discourse_topic_content':
            table_names = [f'{params["index"]}_posts', f'{params["index"]}_info']
        else:
            table_names = [params["index"]]

        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)
        table_templates = Variable.get("need_init_discourse_ck_template", deserialize_json=True)

        template = {}
        for table_name in table_names:
            # TODO; For every new clickhouse table, Need add template.
            for table_template in table_templates:
                if table_template.get("table_name") == table_name:
                    template = table_template.get("temp")
                    break

            df = pd.json_normalize(template)
            template = init_ck_transfer_data.parse_data_init(df)

            if table_name.startswith("discourse"):
                transfer_data = init_ck_transfer_data.transfer_data_by_repo(
                    clickhouse_server_info=clickhouse_server_info,
                    opensearch_index=opensearch_index,
                    table_name=table_name,
                    opensearch_conn_datas=opensearch_conn_datas,
                    template=template, search_key=search_key, transfer_type='discourse')

        return "transfer_discourse_data_to_clickhouse:::end"
    
    from airflow.models import Variable
    need_init_discourse_transfer_to_clickhouse = Variable.get("need_init_discourse_transfer_to_clickhouse", deserialize_json=True)

    for index_and_repo in need_init_discourse_transfer_to_clickhouse:
        if index_and_repo["index"].startswith('discourse'):
            # If u want transfer special table, write IF here.
            for repo in index_and_repo["repo_list"]:
                op_do_ck_transfer_data_by_repo = PythonOperator(
                                task_id=f'do_ck_transfer_os_index_{index_and_repo["index"]}_project_name_{repo.get("project_name")}_mail_list_name_{repo.get("mail_list_name")}',
                                python_callable=transfer_discourse_data_to_clickhouse,
                                op_kwargs={'params': index_and_repo, 'search_key': repo},
                            )
                op_init_transfer_discourse_data_to_clickhouse >> op_do_ck_transfer_data_by_repo