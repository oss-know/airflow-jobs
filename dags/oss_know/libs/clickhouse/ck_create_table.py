import datetime
import re
import numpy
from oss_know.libs.util.log import logger
from oss_know.libs.util.clickhouse_driver import CKServer


# 这个方法是映射ck中的数据类型
def clickhouse_type(data_type):
    type_init = "String"
    if isinstance(data_type, str):
        if validate_iso8601(data_type):
            type_init = "DateTime64(3)"
    elif isinstance(data_type, int):
        type_init = "Int64"
    elif isinstance(data_type, float):
        type_init = "Float64"
    elif isinstance(data_type, list):
        if isinstance(data_type[0], str):
            if validate_iso8601(data_type[0]):
                type_init = "Array(DateTime64(3))"
            else:
                type_init = "Array(String)"
        elif isinstance(data_type[0], int):
            type_init = "Array(Int64)"
        elif isinstance(data_type, float):
            type_init = "Array(Float64)"
    return type_init


# 数据过滤一下
def alter_data_type(row):
    if isinstance(row, numpy.int64):
        row = int(row)
    elif isinstance(row, numpy.float64):
        row = float(row)
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
        # datetime.fromisoformat(dt_str)
        datetime.datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')
    except:
        return False
    return True


def create_ck_table(df,
                    distributed_key="rand()",
                    database_name="default",
                    table_name="default_table",
                    cluster_name="",
                    table_engine="MergeTree",
                    order_by=[],
                    partition_by="",
                    clickhouse_server_info=None):
    if not distributed_key:
        distributed_key = "rand()"
    if not database_name:
        database_name="default"
    # 存储最终的字段
    ck_data_type = []
    # 确定每个字段的类型 然后建表
    for index, row in df.iloc[0].iteritems():
        # 去除包含raw_data的前缀
        if index.startswith('raw_data'):
            index = index[9:]
        index = index.replace('.', '__')
        # 设定一个初始的类型
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
        # # 不是列表判断是否为int类型 可以不用判断是否为字符串类型, 默认是字符串类型
        # elif isinstance(row, int):
        #     data_type_outer = f"`{index}` Int64"
        # elif isinstance(row, str):
        #     if validate_iso8601(row):
        #         data_type_outer = f"`{index}` DateTime64(3)"
        else:
            data_type_outer = f"`{index}` {clickhouse_type(row)}"
        # 将所有的类型都放入这个存储器列表
        ck_data_type.append(data_type_outer)
        # dict1[index] = row
    result = ",\r\n".join(ck_data_type)

    ## 用于删除表格
    # if table_name not in ["discourse_category", "discourse_topic_list", "discourse_topic_content", "discourse_topic_content_posts"]:
    #     drop_local_table_ddl = f'DROP TABLE  {database_name}.{table_name}_local SYNC'
    #     drop_distributed_ddl = f'DROP TABLE  {database_name}.{table_name} SYNC'

    create_local_table_ddl = f'CREATE TABLE IF NOT EXISTS {database_name}.{table_name}_local on cluster {cluster_name}({result}) Engine={table_engine}'
    create_distributed_ddl = f'CREATE TABLE IF NOT EXISTS {database_name}.{table_name} on cluster {cluster_name} ({result}) Engine= Distributed({cluster_name},{database_name},{table_name}_local,{distributed_key})'
    if partition_by:
        create_local_table_ddl = f'{create_local_table_ddl} PARTITION BY {partition_by}'
    if order_by:
        order_by_str = ""
        for i in range(len(order_by)):
            if i != len(order_by) - 1:
                order_by_str = f'{order_by_str}{order_by[i]},'
            else:
                order_by_str = f'{order_by_str}{order_by[i]}'
        create_local_table_ddl = f'{create_local_table_ddl} ORDER BY ({order_by_str})'
    logger.info(f'ddl sql::{create_local_table_ddl}')
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    
    ## 用于删除表格
    # if table_name not in ["discourse_category", "discourse_topic_list", "discourse_topic_content", "discourse_topic_content_posts"]:
    #     execute_ddl(ck, drop_local_table_ddl)
    #     execute_ddl(ck, drop_distributed_ddl)

    execute_ddl(ck, create_local_table_ddl)
    execute_ddl(ck, create_distributed_ddl)
    ck.close()
    return create_local_table_ddl


def execute_ddl(ck: CKServer, sql):
    result = ck.execute_no_params(sql)
    logger.info(f"执行sql后的结果{result}")
