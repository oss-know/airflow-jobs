import time
import datetime
import numpy
from loguru import logger
from opensearchpy import helpers
from pandas import json_normalize
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_RAW_DATA
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data, get_table_structure, ck_check_point, \
    utc_timestamp, transfer_data
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.clickhouse_driver import CKServer




def get_data_from_opensearch(opensearch_index, opensearch_conn_datas, clickhouse_table):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    # 获取上一次的检查点
    response = opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA, body={
        "size": 1,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "search_key.type.keyword": {
                                "value": "os_ck"
                            }
                        }
                    },
                    {
                        "term": {
                            "search_key.opensearch_index.keyword": {
                                "value": opensearch_index
                            }
                        }
                    },
                    {
                        "term": {
                            "search_key.clickhouse_table.keyword": {
                                "value": clickhouse_table
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "search_key.update_timestamp": {
                    "order": "desc"
                }
            }
        ]
    })
    if response['hits']['hits']:
        last_check_timestamp = response['hits']['hits'][0]["_source"]["os_ck"]["last_data"]["updated_at"]
        logger.info(f"上一次的检查点的时间戳是{last_check_timestamp}")
    else:
        logger.error("没找到上一次检查点的任何信息")

    results = helpers.scan(client=opensearch_client, query={
        "query": {
            "bool": {
                "filter": [
                    {"range": {

                        "search_key.updated_at": {


                            "gt": last_check_timestamp
                        }
                    }}
                ]
            }
        }
    }, index=opensearch_index)
    return results, opensearch_client





def sync_transfer_data(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    fields = get_table_structure(table_name=table_name, ck=ck)
    opensearch_datas = get_data_from_opensearch(opensearch_index=opensearch_index,
                                                opensearch_conn_datas=opensearch_conn_datas,
                                                clickhouse_table=table_name)

    max_timestamp = 0
    for os_data in opensearch_datas[0]:
        updated_at = os_data["_source"]["search_key"]["updated_at"]
        if updated_at > max_timestamp:
            max_timestamp = updated_at
        df = json_normalize(os_data["_source"])
        dict_data = parse_data(df)
        fields_have_no_data = []
        for field in fields:
            if not dict_data.get(field):
                fields_have_no_data.append(f'`{field}`')
            if dict_data.get(field) and fields.get(field) == 'DateTime64(3)':
                dict_data[field] = utc_timestamp(dict_data[field])

        # 这里fields_have_no_data 里存储的就是表结构中有的fields 而数据中没有的字段
        if fields_have_no_data:
            logger.info(f'缺失的字段列表：{fields_have_no_data}')
            except_fields = f'EXCEPT({",".join(fields_have_no_data)})'
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
