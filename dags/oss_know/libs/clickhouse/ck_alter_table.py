import datetime
import re
import numpy
from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.data_transfer import np_type_2_py_type, validate_iso8601
from oss_know.libs.util.log import logger


def create_ck_table(df,
                    distributed_key="rand()",
                    database_name="default",
                    table_name="default_table",
                    cluster_name="",
                    clickhouse_server_info=None):
    all_fields = {}
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
        row = np_type_2_py_type(row)
        # 如果row的类型是列表
        if isinstance(row, list):
            # 解析列表中的内容
            # 如果是字典就将 index声明为nested类型的
            # 拿出数组中的一个，这种方式需要保证包含数据，如果数据不全就会出问题
            if row:
                if isinstance(row[0], dict):
                    # type_list存储所有数组中套字典中字典的类型
                    for key in row[0]:
                        # 再进行类型转换一次，可能有bool类型和Nonetype
                        one_of_field = np_type_2_py_type(row[0].get(key))
                        ck_type = np_type_2_py_type(data_type=one_of_field)
                        # 拼接字段和类型
                        data_type_outer = f"`{index}.{key}` Array({ck_type})"
                        all_fields[f'{index}.{key}'] = f'Array({ck_type})'
                        ck_data_type.append(data_type_outer)
                else:
                    one_of_field = np_type_2_py_type(row[0])
                    ck_type = np_type_2_py_type(one_of_field)
                    data_type_outer = f"`{index}` Array({ck_type})"
                    all_fields[f'{index}'] = f'Array({ck_type})'
                    ck_data_type.append(data_type_outer)
        else:
            data_type_outer = f"`{index}` {np_type_2_py_type(row)}"
            # 将所有的类型都放入这个存储器列表
            ck_data_type.append(data_type_outer)
            all_fields[f'{index}'] = f'{np_type_2_py_type(row)}'

    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    old_table_structure = get_table_structure(table_name, ck)
    field_change = {}
    field_delete = []
    for key in old_table_structure:
        if all_fields.get(key) is not None:
            if old_table_structure.get(key) != all_fields.get(key):
                field_change[key] = all_fields.get(key)
            del all_fields[key]
        else:
            field_delete.append(key)
    for key in all_fields:
        sql = f'ALTER TABLE {database_name}.{table_name}_local ON CLUSTER {cluster_name} ADD COLUMN {key} {all_fields.get(key)};'
        ck.execute_no_params(sql)
        logger.info(f'执行的sql语句: {sql}')

    for field in field_delete:
        sql = f'ALTER TABLE {database_name}.{table_name}_local ON CLUSTER {cluster_name} DROP COLUMN {field};'
        ck.execute_no_params(sql)
        logger.info(f'执行的sql语句: {sql}')
    for key in field_change:
        sql = f'ALTER TABLE {database_name}.{table_name}_local ON CLUSTER {cluster_name} MODIFY COLUMN IF EXISTS {key} {field_change.get(key)}'
        ck.execute_no_params(sql)
        logger.info(f'执行的sql语句: {sql}')

    sql = f'DROP TABLE {database_name}.{table_name} ON CLUSTER {cluster_name}'
    ck.execute_no_params(sql)
    sql = f'CREATE TABLE {database_name}.{table_name} ON CLUSTER {cluster_name} AS {database_name}.{table_name}_local Engine= Distributed({cluster_name},{database_name},{table_name}_local,{distributed_key});'
    ck.execute_no_params(sql)
    ck.close()


def execute_ddl(ck: CKServer, sql):
    result = ck.execute_no_params(sql)
    logger.info(f"执行sql后的结果{result}")


def get_table_structure(table_name, ck: CKServer):
    sql = f"DESC {table_name}"
    fields_structure = ck.execute_no_params(sql)
    fields_structure_dict = {}
    for field_structure in fields_structure:
        if field_structure:
            fields_structure_dict[field_structure[0]] = field_structure[1]
        else:
            logger.info("表结构中没有数据")
    logger.info(fields_structure_dict)
    return fields_structure_dict
