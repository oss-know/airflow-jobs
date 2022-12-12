from datetime import datetime
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW
from oss_know.libs.base_dict.variable_key import DAILY_SYNC_GITS_EXCLUDES

from oss_know.libs.util.opensearch_api import OpensearchAPI
from oss_know.libs.github.sync_gits import sync_git_datas
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

opensearch_conn_info = Variable.get("opensearch_conn_data", deserialize_json=True)

with DAG(dag_id='daily_gits_sync',  # schedule_interval='*/5 * * * *',
         schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False,
         tags=['github', 'daily sync'], ) as dag:
    def op_init_daily_gits_sync():
        return 'Start init_daily_gits_sync'


    op_init_daily_gits_sync = PythonOperator(task_id='op_init_daily_gits_sync',
                                             python_callable=op_init_daily_gits_sync, )


    def do_sync_git_info(params):
        from airflow.models import Variable
        owner = params["owner"]
        repo = params["repo"]
        url = params["url"]
        proxy_config = params.get("proxy_config")
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        git_save_local_path = Variable.get("git_save_local_path", deserialize_json=True)
        sync_git_datas(url, owner=owner, repo=repo, proxy_config=proxy_config,
                       opensearch_conn_datas=opensearch_conn_datas, git_save_local_path=git_save_local_path)
        return 'do_sync_git_info:::end'


    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)
    opensearch_api = OpensearchAPI()

    excludes = Variable.get(DAILY_SYNC_GITS_EXCLUDES, deserialize_json=True, default_var=None)
    uniq_owner_repos = opensearch_api.get_uniq_owner_repos(opensearch_client, OPENSEARCH_GIT_RAW, excludes)
    for uniq_item in uniq_owner_repos:
        owner = uniq_item['owner']
        repo = uniq_item['repo']
        origin = uniq_item['origin']

        op_do_init_sync_git_info = PythonOperator(task_id=f'do_sync_git_info_{owner}_{repo}',
                                                  python_callable=do_sync_git_info,
                                                  op_kwargs={'params': {"url": origin, "owner": owner, "repo": repo,
                                                                        "proxy_config": None, }}, )
        op_init_daily_gits_sync >> op_do_init_sync_git_info
