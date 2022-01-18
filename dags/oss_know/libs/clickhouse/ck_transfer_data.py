import datetime
import time
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
        # print(result)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        # print(result)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        print(result)

    def close(self):
        self.client.disconnect()


def clickhouse_type(data_type):
    type_init = "String"
    if isinstance(data_type, int):
        type_init = "Int32"
    return type_init


# 转换为基本数据类型
def alter_data_type(row):
    if isinstance(row, numpy.int64):
        row = int(row)
    elif isinstance(row, dict):
        row = str(row).replace(": ",":")
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
    fields = get_table_structure(table_name=table_name, ck=ck)
    opensearch_datas = get_data_from_opensearch(index=opensearch_index,
                                                opensearch_conn_datas=opensearch_conn_datas)
    # sql = f"INSERT INTO {table_name} VALUES"
    for os_data in opensearch_datas:
        df = json_normalize(os_data["_source"])
        dict_data = parse_data(df)
        # ck.execute(sql, [dict_data])
        fields_have_no_data = []
        # if data_is_none_list:
        #     for i in data_is_none_list:
        #         for k in fields:
        #             if k.startswith(i):
        #                 fields_have_no_data.append(f'`{k}`')
        for field in fields:
            if not dict_data.get(field):
                fields_have_no_data.append(f'`{field}`')
            if dict_data.get(field) and fields.get(field) == 'DateTime64(3)':
                dict_data[field] = date_format_change(dict_data[field])
                print(dict_data[field], "***********************************************************************")
        # 这里fields_have_no_data 里存储的就是表结构中有的fields 而数据中没有的字段
        if fields_have_no_data:
            logger.info(f'缺失的字段列表：{fields_have_no_data}')
            except_fields = f'EXCEPT({",".join(fields_have_no_data)})'
            sql = f"INSERT INTO {table_name} (* {except_fields}) VALUES"
        else:
            sql = f"INSERT INTO {table_name} VALUES"
        logger.info(f'执行的sql语句: {sql} ({dict_data})')
        for key in dict_data:
            print(key,dict_data[key])
        result = ck.execute(sql, [dict_data])
        logger.info(f'执行sql后受影响的行数: {result}')
    ck.close()


def get_data_from_opensearch(index, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client, query={
        "query": {"match_all": {}}
    },
                           index=index)
    return results


def parse_data(df):
    # 这个是最终插入ck的数据字典
    dict_data = {}
    data_is_none_list = []
    for index, row in df.iloc[0].iteritems():
        # 去除以raw_data开头的字段
        if index.startswith('row_data'):
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
            # 这里会先判断解析的第一层列表是否为空的
            if row:
                if isinstance(row[0], dict):
                    for key in row[0]:
                        data_name = f'{index}.{key}'
                        dict_data[data_name] = []
                    for dictt in row:
                        for key in dictt:
                            dictt_data = alter_data_type(dictt.get(key))
                            dict_data.get(f'{index}.{key}').append(dictt_data)
                else:
                    # 这种是数组类型
                    data_name = f'{index}'
                    dict_data[data_name] = row
            else:
                # list为空
                # list为空那么 这里的所有key和value 都会有问题
                # 所以key和value不会记录在dict_data 的数据中
                # 这种把index记录下来
                data_is_none_list.append(index)
        else:
            # 这种是非list类型
            data_name = f'{index}'
            dict_data[data_name] = row

    return dict_data


def get_table_structure(table_name, ck: CKServer):
    sql = f"DESC {table_name}"
    fields_structure = ck.execute_no_params(sql)
    # print(type(fields_structure[0]))
    fields_structure_dict = {}
    # 将表结构中的字段名拿出来
    for field_structure in fields_structure:
        if field_structure:
            fields_structure_dict[field_structure[0]] = field_structure[1]
        else:
            print("i为空")
    logger.info(fields_structure_dict)
    # try:
    #     print(fields_list.index('login1'))
    # except ValueError as e:
    #     logger.error(e)
    return fields_structure_dict


def date_format_change(date):
    format_date = time.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
    return int(time.mktime(format_date) * 1000)
