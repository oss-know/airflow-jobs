# -*-coding:utf-8-*-
import os
import time
from oss_know.libs.github.obs_api import get_files_path, upload2obs


def output_script(index, time_point, ak, sk, init_or_sync='sync'):
    xx = os.system(f"""/opt/airflow/dags/oss_know/libs/github/output_script_v2 {index} {time_point} {init_or_sync}""")
    time.sleep(2)
    print(xx)
    if xx != 0:
        raise Exception("执行脚本出现问题")
    up_2_obs(index, ak, sk)


def up_2_obs(index, ak, sk):
    access_key = ak
    security_key = sk
    bucket_name = 'oss-know-bj'
    file_paths = get_files_path(index)
    print(len(file_paths), '============')
    for file_path in file_paths:
        print(file_path, '---------------------')
        split_path = file_path.split('/')
        obj_name = 'opensearch_data/' + '/'.join(split_path[-2:])
        upload2obs(bucket_name=bucket_name,
                   file_path=file_path,
                   obj_name=obj_name,
                   ak=access_key,
                   sk=security_key)

# output_script('gits',1)
