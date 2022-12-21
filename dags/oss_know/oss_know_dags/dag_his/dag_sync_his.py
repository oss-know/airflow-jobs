from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# git_init_sync_v0.0.3
from opensearchpy import helpers
from airflow.models import XCom
from oss_know.libs.base_dict.variable_key import NEED_INIT_GITS, SYNC_PROFILES_TASK_NUM
from oss_know.libs.obs.obs_api import list_objs, get_obs_client, download_from_obs, excute_script
from oss_know.libs.util.base import get_opensearch_client
from airflow.utils.db import provide_session


@provide_session
def cleanup_xcom(context, session=None):
    # dag_id = context['ti']['dag_id']
    dag_id = 'dag_sync_his'
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


with DAG(
        dag_id='dag_sync_his',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['his'],
        on_success_callback=cleanup_xcom
) as dag:
    def init_sync_his(ds, **kwargs):
        return 'Start dag_sync_his'


    op_init_sync_his = PythonOperator(
        task_id='init_sync_his',
        python_callable=init_sync_his,
    )


    def do_list_obs_data(params, **kwargs):
        from airflow.models import Variable
        ak_sk = Variable.get("obs_ak_sk", deserialize_json=True)
        server = 'http://obs.cn-north-4.myhuaweicloud.com'
        ak = ak_sk['ak']
        sk = ak_sk['sk']
        bucketname = 'oss-know-bj'
        prefix = 'opensearch_data'
        obs_client = get_obs_client(ak, sk, server=server,
                                    proxy_host=1,
                                    proxy_port=1,
                                    proxy_username=1,
                                    proxy_password=1)
        objs = list_objs(bucketname=bucketname, obs_client=obs_client, prefix=prefix)
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
        results = helpers.scan(client=opensearch_client,
                               query={
                                   "query": {
                                       "term": {
                                           "if_successful": {
                                               "value": "true"
                                           }
                                       }
                                   }
                               }, index='check_download_obs_data',
                               size=5000,
                               scroll="20m",
                               request_timeout=120,
                               preserve_order=True)
        already_successful_objs = []
        need_download_objs = []
        for result in results:
            already_successful_objs.append(result["_source"]["obj_name"])
        for obj in objs:
            if obj not in already_successful_objs:
                need_download_objs.append(obj)
        print(need_download_objs)
        print(len(need_download_objs))
        kwargs['ti'].xcom_push(key=f'objs_need_to_download', value=need_download_objs)
        return 'do_list_obs_data:::end'


    def do_download_data_from_obs(params, **kwargs):
        from airflow.models import Variable
        ak_sk = Variable.get("obs_ak_sk", deserialize_json=True)
        objs_need_to_download = kwargs['ti'].xcom_pull(key=f'objs_need_to_download')
        objs_need_to_download = [
            'opensearch_data/data_2022-07-08/github_issues_comments_init_2022-07-08T07-28-25Z+0000.json.gzip']

        ak = ak_sk['ak']
        sk = ak_sk['sk']
        server = 'http://obs.cn-north-4.myhuaweicloud.com'
        obs_client = get_obs_client(ak, sk, server=server,
                                    proxy_host=None,
                                    proxy_port=None,
                                    proxy_username=None,
                                    proxy_password=None)
        bucketname = 'oss-know-bj'
        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
        for obj in objs_need_to_download:
            download_from_obs(obs_client=obs_client, bucketname=bucketname, objectname=obj,
                              opensearch_client=opensearch_client)
        return 'do_sync_git_info:::end'


    def excute_elasticdump_input_script():
        from airflow.models import Variable

        opensearch_conn_datas = Variable.get("opensearch_conn_data", deserialize_json=True)
        opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
        # 一个index 是记录从obs下载的记录，另外一个是记录从压缩包导入opensearch的情况
        download_objs = helpers.scan(client=opensearch_client,
                                     query={
                                         "query": {
                                             "term": {
                                                 "if_successful": {
                                                     "value": "true"
                                                 }
                                             }
                                         }
                                     }, index='check_download_obs_data',
                                     size=5000,
                                     scroll="20m",
                                     request_timeout=120,
                                     preserve_order=True)
        already_insert_into_opensearch_objs = helpers.scan(client=opensearch_client,
                                                           query={
                                                               "query": {
                                                                   "term": {
                                                                       "if_successful": {
                                                                           "value": "true"
                                                                       }
                                                                   }
                                                               }
                                                           }, index='check_insert_into_os_objs',
                                                           size=5000,
                                                           scroll="20m",
                                                           request_timeout=120,
                                                           preserve_order=True)
        already_successful_download_objs = []
        for result in download_objs:
            already_successful_download_objs.append(result["_source"]["obj_name"])
        already_insert_into_opensearch_objs_list = []
        for result in already_insert_into_opensearch_objs:
            already_insert_into_opensearch_objs_list.append(result["_source"]["obj_name"])
        need_insert_objs = []
        for obj in already_successful_download_objs:
            if obj not in already_insert_into_opensearch_objs_list:
                need_insert_objs.append(obj)
        need_insert_objs = [
            'opensearch_data/data_2022-07-08/github_issues_comments_init_2022-07-08T07-28-25Z+0000.json.gzip']
        for obj in need_insert_objs:
            excute_script(obj_name=obj, opensearch_client=opensearch_client)


    from airflow.models import Variable

    op_do_list_obs_data = PythonOperator(
        task_id=f'do_list_obs_data',
        python_callable=do_list_obs_data,
    )
    op_do_download_data_from_obs = PythonOperator(
        task_id=f'do_download_data_from_obs',
        python_callable=do_download_data_from_obs,
        provide_context=True
    )

    op_excute_elasticdump_input_script = PythonOperator(
        task_id=f'excute_elasticdump_input_script',
        python_callable=excute_elasticdump_input_script,
        provide_context=True
    )

    op_init_sync_his >> op_do_list_obs_data >> op_do_download_data_from_obs >> op_excute_elasticdump_input_script
