from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITS, OPENSEARCH_CONN_DATA, \
    GIT_SAVE_LOCAL_PATH, CK_TABLE_DEFAULT_VAL_TPLT, CLICKHOUSE_DRIVER_INFO
from oss_know.libs.clickhouse import init_ck_transfer_data
from oss_know.libs.github.init_gits import init_gits_repo
from oss_know.libs.util.base import unify_gits_origin

# gits_init_v0.0.3
with DAG(
        dag_id='git_init_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    opensearch_conn_info = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)
    clickhouse_server_info = Variable.get(CLICKHOUSE_DRIVER_INFO, deserialize_json=True)


    def do_init_gits_repo(owner, repo, origin):
        git_save_local_path = Variable.get(GIT_SAVE_LOCAL_PATH, deserialize_json=True)
        init_gits_repo(git_url=origin,
                       owner=owner,
                       repo=repo,
                       proxy_config=None,
                       opensearch_conn_datas=opensearch_conn_info,
                       git_save_local_path=git_save_local_path)
        return 'do_init_gits:::end'


    def do_ck_transfer(owner, repo):
        search_key = {"owner": owner, "repo": repo}
        table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)

        template = table_templates.get(OPENSEARCH_GIT_RAW)
        init_ck_transfer_data.transfer_data_by_repo(
            clickhouse_server_info=clickhouse_server_info,
            opensearch_index=OPENSEARCH_GIT_RAW,
            table_name=OPENSEARCH_GIT_RAW,
            opensearch_conn_datas=opensearch_conn_info,
            template=template, owner_repo_or_project_maillist_name=search_key,
            transfer_type='github_git_init_by_repo')


    git_info_list = Variable.get(NEED_INIT_GITS, deserialize_json=True)
    for git_info in git_info_list:
        owner = git_info["owner"]
        repo = git_info["repo"]
        url = git_info["url"]
        op_init_gits_repo = PythonOperator(
            task_id=f'do_init_gits_{owner}_{repo}',
            python_callable=do_init_gits_repo,
            op_kwargs={
                'owner': owner,
                'repo': repo,
                'origin': unify_gits_origin(url)
            },
        )

        op_transfer_data_to_ck = PythonOperator(
            task_id=f'do_transfer_data_to_ck_{owner}_{repo}',
            python_callable=do_ck_transfer,
            op_kwargs={
                'owner': owner,
                'repo': repo,
            },
        )

        op_init_gits_repo >> op_transfer_data_to_ck
