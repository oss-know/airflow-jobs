# -*-coding:utf-8-*-
import os
import datetime
import time
from obs import ObsClient


def get_files_path(index):
    """获取最新一天dump下来的opensearch data file name"""
    tmp = os.popen('ls /opt/airflow/dags/oss_know/libs/github | grep data').readlines()
    file_names = []

    for file_name in tmp:
        file_names.append(file_name[0:-1])
    ctime_list = []
    for file_name in file_names:
        ctimestamp = os.path.getctime(f'/opt/airflow/dags/oss_know/libs/github/{file_name}')
        time_tuple = time.localtime(ctimestamp)
        ctime = time.strftime('%Y-%m-%d', time_tuple)
        # ctime = datetime.datetime.fromtimestamp(int(ctimestamp))
        ctime_list.append(ctime)
    dir_name = 'data_' + max(ctime_list)
    indices = os.popen(f'ls /opt/airflow/dags/oss_know/libs/github/{dir_name}')
    parent_path = os.popen(f'cd /opt/airflow/dags/oss_know/libs/github/{dir_name} && pwd').readlines()[0][0:-1]
    opensearch_data_file_names = []
    for opensearch_data_file_name in indices:
        if opensearch_data_file_name[0:-1].startswith(index):
            opensearch_data_file_names.append(parent_path + '/' + opensearch_data_file_name[0:-1])
    return opensearch_data_file_names


def get_obs_client(ak, sk):
    obsClient = ObsClient(
        access_key_id=ak,  # 刚刚下载csv文件里面的Access Key Id
        secret_access_key=sk,  # 刚刚下载csv文件里面的Secret Access Key
        server='http://obs.cn-north-4.myhuaweicloud.com'  # 这里的访问域名就是我们在桶的基本信息那里记下的东西
    )
    return obsClient


def get_file_size(file_path):
    """函数返回值刚开始是字节"""
    fsize = os.path.getsize(file_path)
    fsize = fsize / float(1024 * 1024)
    """返回的单位是MB"""
    return fsize


def callback(transferredAmount, totalAmount, totalSeconds):
    # 获取上传平均速率(MB/S)
    print(f"平均速率:{format(transferredAmount * 1.0 / totalSeconds / 1024 / 1024, '.3')} MB/S")
    # 获取上传进度百分比
    print(f"上传进度百分比{format(transferredAmount * 1.0 / totalAmount, '.2%')}")


def get_part_count_and_size(file_path):
    file_size = get_file_size(file_path)
    print(file_path, file_size)
    part_count = 0
    part_size = 0
    if file_size >= 4 * 1024:
        part_count = int(file_size / (3 * 1024)) + 1
        # 3g  单位是字节
        part_size = 31457280
    else:
        part_count = 1
        part_size = None
    return part_count, part_size


def upload2obs(bucket_name, file_path, obj_name, ak, sk):
    part_count_and_size = get_part_count_and_size(file_path=file_path)
    part_count = part_count_and_size[0]
    part_size = part_count_and_size[1]
    # print(part_count,part_size)
    obsClient = get_obs_client(ak, sk)
    try:
        uploadId = None
        etags = {}
        for etag in range(part_count):
            etags[f'etag{etag + 1}'] = None

        # 初始化上传段任务
        resp = obsClient.initiateMultipartUpload(bucket_name, obj_name,
                                                 contentType='text/plain')
        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('bucketName:', resp.body.bucketName)
            print('objectKey:', resp.body.objectKey)
            print('uploadId:', resp.body.uploadId)
            uploadId = resp.body.uploadId
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
        part_num = 1
        offset = 0

        for etag in etags:
            # 上传段
            resp = obsClient.uploadPart(bucket_name, obj_name,
                                        part_num,
                                        uploadId,
                                        file_path,
                                        isFile=True,
                                        offset=offset,
                                        partSize=part_size,
                                        progressCallback=callback)

            if part_size:
                part_num += 1
                offset = offset + part_size
            if resp.status < 300:
                print('requestId:', resp.requestId)
                print('etag:', resp.body.etag)
                etags[etag] = resp.body.etag
            else:
                print('errorCode:', resp.errorCode)
                print('errorMessage:', resp.errorMessage)

        # 合并段
        from obs import CompleteMultipartUploadRequest, CompletePart

        parts = []
        part_num = 1
        for etag in etags:
            parts.append(CompletePart(partNum=part_num, etag=etags[etag]))
            part_num += 1

        completeMultipartUploadRequest = CompleteMultipartUploadRequest(parts=parts)

        resp = obsClient.completeMultipartUpload(bucket_name, obj_name, uploadId,
                                                 completeMultipartUploadRequest)

        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('etag:', resp.body.etag)
            print('bucket:', resp.body.bucket)
            print('key:', resp.body.key)
            print('location:', resp.body.location)
            print('versionId:', resp.body.versionId)
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
    except:
        import traceback

        print(traceback.format_exc())
    obsClient.close()


def download_from_obs(ak, sk):
    obsClient = get_obs_client(ak, sk)
    try:
        resp = obsClient.getObject('bucketname', 'objectname', downloadPath='localfile')
        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('url:', resp.body.url)
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
    except:
        import traceback
        print(traceback.format_exc())


def list_objs(ak, sk, bucketname):
    obsClient = get_obs_client(ak, sk)
    try:
        resp = obsClient.listObjects(bucketname,prefix='opensearch_data', max_keys=100)
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
    obsClient.close()


def list_multipart_uploads(ak, sk, bucketname):
    obsClient = get_obs_client(ak, sk)
    try:
        from obs import ListMultipartUploadsRequest
        multipart = ListMultipartUploadsRequest(prefix='20', max_uploads=10)

        resp = obsClient.listMultipartUploads(bucketname, multipart)

        if resp.status < 300:
            print('requestId:', resp.requestId)
            print('bucket:', resp.body.bucket)
            print('prefix:', resp.body.prefix)
            print('maxUploads:', resp.body.maxUploads)
            print('isTruncated:', resp.body.isTruncated)
            index = 1
            for upload in resp.body.upload:
                print('upload [' + str(index) + ']')
                print('key:', upload.key)
                print('uploadId:', upload.uploadId)
                print('storageClass:', upload.storageClass)
                print('initiated:', upload.initiated)
                print('owner_id:', upload.owner.owner_id)
                print('owner_name:', upload.owner.owner_name)
                index += 1
        else:
            print('errorCode:', resp.errorCode)
            print('errorMessage:', resp.errorMessage)
    except:
        import traceback
        print(traceback.format_exc())
