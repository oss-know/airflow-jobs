# -*-coding:utf-8-*-
import datetime
import os
import shutil
import time

from obs import ObsClient


def callback(transferredAmount, totalAmount, totalSeconds):
    # 获取上传平均速率(MB/S)
    print(f"平均速率:{format(transferredAmount * 1.0 / totalSeconds / 1024 / 1024, '.3')} MB/S")
    # 获取上传进度百分比
    print(f"下载或上传进度百分比{format(transferredAmount * 1.0 / totalAmount, '.2%')}")


def get_obs_client(ak, sk, server=None, proxy_host=None, proxy_port=None, proxy_username=None, proxy_password=None):
    obsClient = ObsClient(
        access_key_id=ak,  # 刚刚下载csv文件里面的Access Key Id
        secret_access_key=sk,  # 刚刚下载csv文件里面的Secret Access Key
        server='http://obs.cn-north-4.myhuaweicloud.com',  # 这里的访问域名就是我们在桶的基本信息那里记下的东西
        # server=server,
        proxy_host=proxy_host,
        proxy_port=proxy_port,
        proxy_username=proxy_username,
        proxy_password=proxy_password
    )
    return obsClient


def list_objs(bucketname, obs_client, prefix):
    """
    列出匹配到对象信息
    :param prefix: 对象名前缀匹配
    """
    try:
        resp = obs_client.listObjects(bucketname, prefix=prefix, max_keys=10000)
        objs = []
        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('name:', resp.body.name)
            print('prefix:', resp.body.prefix)
            print('max_keys:', resp.body.max_keys)
            print('is_truncated:', resp.body.is_truncated)
            index = 1
            for content in resp.body.contents:
                objs.append(content.key)
                print('object [' + str(index) + ']')
                print('key:', content.key)
                print('lastModified:', content.lastModified)
                print('etag:', content.etag)
                print('size:', content.size)
                print('storageClass:', content.storageClass)
                print('owner_id:', content.owner.owner_id)
                print('owner_name:', content.owner.owner_name)
                index += 1
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
    except:
        import traceback
        print(traceback.format_exc())
    print(objs)
    return objs


def download_from_obs(obs_client, bucketname, objectname, opensearch_client):
    timestamp = time.time()
    download_path = f'/opt/airflow/csv/opensearch_data/{objectname.split("/")[-2]}/{objectname.split("/")[-1]}'
    # 若路径已经存在那么删除
    if os.path.exists(download_path):
        os.remove(download_path)
    try:
        resp = obs_client.getObject(bucketname,
                                    objectname,
                                    downloadPath=download_path,
                                    progressCallback=callback)
        opensearch_client.delete_by_query(index='check_download_obs_data', body={
            "query": {
                "term": {
                    "obj_name.keyword": {
                        "value": objectname
                    }
                }
            }
        })

        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('url:', resp.body.url)

            response = opensearch_client.index(
                index="check_download_obs_data",
                body={
                    "obj_name": objectname,
                    "download_datetime": timestamp_to_utc(int(timestamp)),
                    "download_timestamp": int(timestamp * 1000),
                    "if_successful": True
                },
                refresh=True
            )
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
            response = opensearch_client.index(
                index="check_download_obs_data",
                body={
                    "obj_name": "",
                    "download_datetime": timestamp_to_utc(int(timestamp)),
                    "download_timestamp": int(timestamp * 1000),
                    "if_successful": False
                },
                refresh=True
            )
    except:
        import traceback
        print(traceback.format_exc())
        response = opensearch_client.index(
            index="check_download_obs_data",
            body={
                "obj_name": "",
                "download_datetime": timestamp_to_utc(int(timestamp)),
                "download_timestamp": int(timestamp * 1000),
                "if_successful": False
            },
            refresh=True
        )


def excute_script(obj_name, opensearch_client):
    timestamp = time.time()
    # obj_name = 'data_2022-06-15/github_issues_2022-06-15T11:21:55Z+0800.json.gzip'
    obj = obj_name.split('/')[-1]
    input_path = "/opt/airflow/csv/" + obj_name
    print(input_path)
    host_port = '192.168.8.110:19201'
    index = ''
    if obj.startswith("gits"):
        index = "gits"
    elif obj.startswith("github_commits"):
        index = "github_commits"
    elif obj.startswith("github_issues_timeline"):
        index = "github_issues_timeline"
    elif obj.startswith("github_issues_comments"):
        index = "github_issues_comments"
    elif obj.startswith("github_issues"):
        index = "github_issues"
    elif obj.startswith("github_profile"):
        index = "github_profile"
    elif obj.startswith("github_pull_requests"):
        index = "github_pull_requests"
    print(index)
    print(f"""https_proxy="" http_proxy="" HTTP_PROXY="" HTTPS_PROXY="" /opt/airflow/dags/oss_know/libs/obs/elasticsearch_dump_input_script.sh {index} {host_port} {input_path}""")
    xx = os.system(f"""https_proxy="" http_proxy="" HTTP_PROXY="" HTTPS_PROXY="" /opt/airflow/dags/oss_know/libs/obs/elasticsearch_dump_input_script.sh {index} {host_port} {input_path}""")
    print(xx)
    # f = os.popen(f"""https_proxy="" http_proxy="" HTTP_PROXY="" HTTPS_PROXY="" /opt/airflow/dags/oss_know/libs/obs/elasticsearch_dump_input_script.sh {index} {host_port} {input_path}""")  # 返回的是一个文件对象
    opensearch_client.delete_by_query(index='check_insert_into_os_objs', body={
        "query": {
            "term": {
                "obj_name.keyword": {
                    "value": obj_name
                }
            }
        }
    })

    if xx != 0:
        response = opensearch_client.index(
            index="check_insert_into_os_objs",
            body={
                "obj_name": obj_name,
                "download_datetime": timestamp_to_utc(int(timestamp)),
                "download_timestamp": int(timestamp * 1000),
                "if_successful": False
            },
            refresh=True
        )
        raise Exception("shell 脚本没有执行成功")
    else:
        response = opensearch_client.index(
            index="check_insert_into_os_objs",
            body={
                "obj_name": obj_name,
                "download_datetime": timestamp_to_utc(int(timestamp)),
                "download_timestamp": int(timestamp * 1000),
                "if_successful": True
            },
            refresh=True
        )


def timestamp_to_utc(timestamp):
    # 10位时间戳
    return datetime.datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%dT%H:%M:%SZ")
