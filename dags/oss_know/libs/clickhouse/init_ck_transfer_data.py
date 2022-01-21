import datetime
import time
import numpy
from loguru import logger
from opensearchpy import helpers
from pandas import json_normalize
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_RAW_DATA
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW, OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.clickhouse_driver import CKServer


def clickhouse_type(data_type):
    type_init = "String"
    if isinstance(data_type, int):
        type_init = "Int64"
    return type_init


def ck_check_point(opensearch_client, opensearch_index, clickhouse_table, updated_at):
    now_time = datetime.datetime.now()
    check_info = {
        "search_key": {
            "type": "os_ck",
            "update_time": now_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "update_timestamp": now_time.timestamp(),
            "opensearch_index": opensearch_index,
            "clickhouse_table": clickhouse_table
        },
        "os_ck": {
            "type": "os_ck",
            "opensearch_index": opensearch_index,
            "clickhouse_table": clickhouse_table,
            "last_data": {
                "updated_at": updated_at
            }
        }
    }
    # 插入一条数据
    response = opensearch_client.index(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA, body=check_info)
    logger.info(response)


# 转换为基本数据类型
def alter_data_type(row):
    if isinstance(row, numpy.int64):
        row = int(row)
    elif isinstance(row, dict):
        row = str(row).replace(": ", ":")
    elif isinstance(row, numpy.bool_):
        row = int(bool(row))
    elif row is None:
        row = "null"
    elif isinstance(row, bool):
        row = int(row)
    else:
        pass
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
    max_timestamp = 0
    for os_data in opensearch_datas[0]:
        updated_at = os_data["_source"]["search_key"]["updated_at"]
        if updated_at > max_timestamp:
            max_timestamp = updated_at
        df = json_normalize(os_data["_source"])
        dict_data = parse_data(df)
        except_fields = []
        for field in fields:
            if not dict_data.get(field):
                except_fields.append(f'`{field}`')
            if dict_data.get(field) and fields.get(field) == 'DateTime64(3)':
                dict_data[field] = utc_timestamp(dict_data[field])

        # 这里except_fields 里存储的就是表结构中有的fields 而数据中没有的字段
        if except_fields:
            logger.info(f'缺失的字段列表：{except_fields}')
            except_fields = f'EXCEPT({",".join(except_fields)})'
            sql = f"INSERT INTO {table_name} (* {except_fields}) VALUES"
        else:
            sql = f"INSERT INTO {table_name} VALUES"
        logger.info(f'执行的sql语句: {sql} ({dict_data})')
        result = ck.execute(sql, [dict_data])
        logger.info(f'执行sql后受影响的行数: {result}')

    # 将检查点放在这里插入
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=max_timestamp)
    ck.close()


def get_data_from_opensearch(index, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client, query={
        "query": {"match_all": {}}
    }, index=index)
    return results, opensearch_client


def parse_data(df):
    # 这个是最终插入ck的数据字典
    dict_data = {}
    for index, row in df.iloc[0].iteritems():
        # 去除以raw_data开头的字段
        if index.startswith(CLICKHOUSE_RAW_DATA):
            index = index[9:]
        # 只要是空的就跳过
        if not row:
            continue
        # 第一步的转化
        row = alter_data_type(row)
        # # 这里的字符串都转换成在ck中能够使用函数解析json的标准格式
        # if isinstance(row, str):
        #     row.replace(": ", ":")
        # 解决嵌套array
        if isinstance(row, list):
            # 数组中是字典
            if isinstance(row[0], dict):
                for key in row[0]:
                    data_name = f'{index}.{key}'
                    dict_data[data_name] = []
                for data in row:
                    for key in data:
                        filter_data = alter_data_type(data.get(key))
                        dict_data.get(f'{index}.{key}').append(filter_data)
            else:
                # 这种是数组类型
                data_name = f'{index}'
                dict_data[data_name] = row
        else:
            # 这种是非list类型
            data_name = f'{index}'
            dict_data[data_name] = row
    return dict_data


def get_table_structure(table_name, ck: CKServer):
    sql = f"DESC {table_name}"
    fields_structure = ck.execute_no_params(sql)
    fields_structure_dict = {}
    # 将表结构中的字段名拿出来
    for field_structure in fields_structure:
        if field_structure:
            fields_structure_dict[field_structure[0]] = field_structure[1]
        else:
            logger.info("表结构中没有数据")
    logger.info(fields_structure_dict)
    return fields_structure_dict


def utc_timestamp(date):
    format_date = time.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
    return int(time.mktime(format_date) * 1000)
