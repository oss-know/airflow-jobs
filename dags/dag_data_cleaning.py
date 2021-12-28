import time
from datetime import datetime
from pprint import pprint
from airflow import DAG
from airflow.operators.python import PythonOperator
from libs.util.base import get_opensearch_client

# git_init_sync_v0.0.3

with DAG(
        dag_id='git_github_profile_data_cleaning_v1',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['github'],
) as dag:
    def init_data_cleaning(ds, **kwargs):
        return 'Start init_data_cleaning'


    op_init_data_cleaning= PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_data_cleaning,
    )
    from airflow.models import Variable

    opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)


    def do_sync_init_data_cleaning(params):
        from libs.data_clean import git_github_profile
        owner = params[0]
        repo = params[1]
        git_github_profile.data_clean(owner=owner,
                                      repo=repo,
                                      opensearch_conn_datas=opensearch_conn_datas)

        return 'do_sync_init_data_cleaning:::end'


    git_info_list = Variable.get("git_info_list", deserialize_json=True)
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = opensearch_client.search(index="git_raw", body={
        "size": 10000,
        "query": {
            "match_all": {}
        }
        , "collapse": {
            "field": "search_key.origin.keyword"
        }
        , "_source": ["search_key.repo", "search_key.owner"]
    }
                             )
    datas = results["hits"]["hits"]
    for commit in datas:
        owner = commit["_source"]["search_key"]["owner"]
        repo = commit["_source"]["search_key"]["repo"]
        owner_repo = [owner,repo]
        op_do_sync_init_data_cleaning = PythonOperator(
            task_id=f'op_do_sync_init_data_cleaning_{owner_repo[0]}_{owner_repo[1]}',
            python_callable=do_sync_init_data_cleaning,
            op_kwargs={'params': owner_repo},
        )

        op_init_data_cleaning >> op_do_sync_init_data_cleaning
