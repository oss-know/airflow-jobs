import datetime
import re
import numpy
import json
import pandas as pd
from loguru import logger
from pandas import json_normalize
from oss_know.libs.util.clickhouse_driver import CKServer


# 这个方法是映射ck中的数据类型
def clickhouse_type(data_type):
    type_init = "String"
    if isinstance(data_type, str):
        if validate_iso8601(data_type):
            type_init = "DateTime64(3)"
    elif isinstance(data_type, int):
        type_init = "Int64"
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


regex = r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'

match_iso8601 = re.compile(regex).match


# 判断是不是iso8601 格式字符串
def validate_iso8601(str_val):
    try:
        if match_iso8601(str_val) is not None:
            return True
    except:
        pass
    return False


# 这里判断字符串是不是标准的日期格式
def datetime_valid(dt_str):
    try:
        datetime.fromisoformat(dt_str)
    except:
        return False
    return True


def create_ck_table(df,
                    table_name="default_table",
                    table_engine="MergeTree",
                    order_by=[],
                    partition_by="",
                    clickhouse_server_info=None):
    # 存储最终的字段
    ck_data_type = []
    # 确定每个字段的类型 然后建表
    for index, row in df.iloc[0].iteritems():
        # 去除包含raw_data的前缀
        if index.startswith('raw_data'):
            index = index[9:]
        # ck中单个字段的字段名称和字段的类型 拼接的字符串
        data_type_outer = f"`{index}` String"
        # 将数据进行类型的转换，有些类型但是pandas中独有的类型
        row = alter_data_type(row)
        # 如果row的类型是列表
        if isinstance(row, list):
            # 解析列表中的内容
            # 如果是字典就将 index声明为nested类型的
            # 拿出数组中的一个，这种方式需要保证包含数据，如果数据不全就会出问题
            if row:
                if isinstance(row[0], dict):
                    # 这个type_list存储所有数组中套字典中字典的类型
                    type_list = []
                    for key in row[0]:
                        # 这里再进行类型转换一次，可能有bool类型和Nonetype
                        one_of_field = alter_data_type(row[0].get(key))
                        # 这里映射ck的类型
                        ck_type = clickhouse_type(data_type=one_of_field)
                        # 拼接字段和类型
                        data_type = f"{key} {ck_type}"
                        type_list.append(data_type)

                    one_nested_type = ",".join(type_list)
                    data_type_outer = f"`{index}` Nested({one_nested_type})"
                else:
                    # 这种就声明为数组就行了
                    one_of_field = alter_data_type(row[0])
                    ck_type = clickhouse_type(one_of_field)
                    data_type_outer = f"`{index}` Array({ck_type})"
        # 不是列表判断是否为int类型 可以不用判断是否为字符串类型, 默认是字符串类型
        elif isinstance(row, int):
            data_type_outer = f"`{index}` Int64"
        elif isinstance(row, str):
            if validate_iso8601(row):
                data_type_outer = f"`{index}` DateTime64(3)"
        # 将所有的类型都放入这个存储器列表
        ck_data_type.append(data_type_outer)
        # dict1[index] = row
    result = ",\r\n".join(ck_data_type)
    create_table_ddl = f'CREATE TABLE IF NOT EXISTS {table_name} ({result}) Engine={table_engine}'
    if partition_by:
        create_table_ddl = f'{create_table_ddl} PARTITION BY {partition_by}'
    if order_by:
        order_by_str = ""
        for i in range(len(order_by)):
            if i != len(order_by) - 1:
                order_by_str = f'{order_by_str}{order_by[i]},'
            else:
                order_by_str = f'{order_by_str}{order_by[i]}'
        create_table_ddl = f'{create_table_ddl} ORDER BY ({order_by_str})'
    logger.info(f'ddl sql::{create_table_ddl}')
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    execute_ddl(ck, create_table_ddl)
    ck.close()
    return create_table_ddl


def execute_ddl(ck: CKServer, sql):
    result = ck.execute_no_params(sql)
    logger.info(f"执行sql后的结果{result}")
