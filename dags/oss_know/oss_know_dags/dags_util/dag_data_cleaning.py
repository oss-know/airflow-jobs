from datetime import datetime
from opensearchpy import helpers as opensearch_helpers
from airflow import DAG
from airflow.operators.python import PythonOperator

from oss_know.libs.base_dict.variable_key import OPENSEARCH_CONN_DATA
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW

from oss_know.libs.util.base import get_opensearch_client

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


    op_init_data_cleaning = PythonOperator(
        task_id='init_sync_git_info',
        python_callable=init_data_cleaning,
    )
    from airflow.models import Variable

    opensearch_conn_datas = Variable.get(OPENSEARCH_CONN_DATA, deserialize_json=True)


    def do_sync_init_data_cleaning(params):
        from oss_know.libs.data_clean import git_github_profile
        owner = params[0]
        repo = params[1]
        git_github_profile.data_clean(owner=owner,
                                      repo=repo,
                                      opensearch_conn_datas=opensearch_conn_datas)

        return 'do_sync_init_data_cleaning:::end'


    # git_info_list = Variable.get("git_info_list", deserialize_json=True)
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    # 拿出opensearch中所有去重后的项目
    results = opensearch_helpers.scan(client=opensearch_client,
                                      index=OPENSEARCH_GIT_RAW,
                                      query={
                                          "query": {
                                              "match_all": {}
                                          }
                                          , "collapse": {
                                              "field": "search_key.origin.keyword"
                                          }
                                          , "_source": ["search_key.repo", "search_key.owner"]
                                      })
    results = opensearch_client.search(index=OPENSEARCH_GIT_RAW,
                                       body={
                                           "size": 0,
                                           "aggs": {
                                               "group_by_owner": {
                                                   "terms": {
                                                       "field": "search_key.owner.keyword",
                                                       "size": 50000
                                                   }, "aggs": {
                                                       "group_by_repo": {
                                                           "terms": {
                                                               "field": "search_key.repo.keyword",
                                                               "size": 50000
                                                           }
                                                       }
                                                   }
                                               }
                                           }
                                       })
    datas = results["aggregations"]["group_by_owner"]["buckets"]
    for owner_repo in datas:
        owner = owner_repo["key"]
        for repo_ in owner_repo["group_by_repo"]["buckets"]:
            repo = repo_["key"]
            owner_repo = [owner, repo]
            op_do_sync_init_data_cleaning = PythonOperator(
                task_id=f'op_do_sync_init_data_cleaning_{owner_repo[0]}_{owner_repo[1]}',
                python_callable=do_sync_init_data_cleaning,
                op_kwargs={'params': owner_repo},
            )

            op_init_data_cleaning >> op_do_sync_init_data_cleaning
