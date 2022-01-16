import copy
import datetime
import shutil
import os
import numpy
import json
import pandas as pd
from loguru import logger
from git import Repo
from clickhouse_driver import Client, connect
from opensearchpy import helpers
from pandas import json_normalize
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.opensearch_api import OpensearchAPI


class CKServer:
    def __init__(self, host, port, user, password, database):
        self.client = Client(host=host, port=port, user=user, password=password, database=database)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        print(result)

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        print(result)

    def fetchall(self, sql):
        result = self.client.execute(sql)
        print(result)

    def close(self):
        self.client.disconnect()


def clickhouse_type(data_type):
    type_init = "String"
    if isinstance(data_type, int):
        type_init = "UInt32"
    return type_init


# 数据过滤一下
def alter_data_type(row):
    if isinstance(row, numpy.int64):
        row = int(row)
    elif isinstance(row, numpy.bool_):
        row = int(bool(row))
    elif row is None:
        row = "null"
    elif isinstance(row, bool):
        row = int(row)
    return row


def transfer_data(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    opensearch_datas = get_data_from_opensearch(index=opensearch_index,
                                                opensearch_conn_datas=opensearch_conn_datas)
    params = []
    sql = f"INSERT INTO {table_name} VALUES"
    for os_data in opensearch_datas:
        # print(os_data["_source"], "*************************************************************")
        df = json_normalize(os_data["_source"])
        dict_data = parse_data(df)
        ck.execute(sql, [dict_data])
    ck.close()


def get_data_from_opensearch(index, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client, query={
        "query": {"match_all": {}}
    }, index=index)
    return results


def parse_data(df):
    # 这个是最终插入ck的数据字典
    dict_data = {}
    for index, row in df.iloc[0].iteritems():
        # 去除以raw_data开头的字段
        if index.startswith('raw_data'):
            index = index[9:]
        # 第一步的转化
        row = alter_data_type(row)
        # 这里的字符串都转换成在ck中能够使用函数解析json的标准格式
        if isinstance(row, str):
            row.replace(": ", ":")
        data_name = ""
        # 解决嵌套array
        if isinstance(row, list):
            # 数组中是字典
            # 这里会有问题，如果数组中没有数据这里就会报数组越界异常
            # 想到的方法：先吧表结构拿出来，如果这个数据都为空那么就把 以index 开头的字段全部去除不插入
            # 现在先假设数据都是全的
            if row:
                if isinstance(row[0], dict):
                    for key in row[0]:
                        data_name = f'{index}.{key}'
                        dict_data[data_name] = []
                    for dictt in row:
                        for key in dictt:
                            dictt_data = alter_data_type(dictt.get(key))
                            dict_data.get(f'{index}.{key}').append(dictt_data)
                            print(type(dictt_data), dictt_data)
                else:
                    # 这种是数组类型
                    data_name = f'{index}'
                    dict_data[data_name] = row
        else:
            # 这种是非list类型
            data_name = f'{index}'
            dict_data[data_name] = row

    return dict_data


def execute_ddl(ck: CKServer, sql):
    result = ck.execute_no_params(sql)
    logger.info(f"执行sql后的结果{result}")
