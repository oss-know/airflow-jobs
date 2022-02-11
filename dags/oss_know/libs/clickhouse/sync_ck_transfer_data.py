import json
import pandas as pd
import psycopg2
from airflow import AirflowException
from loguru import logger
from opensearchpy import helpers
from opensearchpy.exceptions import NotFoundError
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data, get_table_structure, ck_check_point, \
    utc_timestamp
from oss_know.libs.util.airflow import get_postgres_conn
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
        last_update_time = response['hits']['hits'][0]["_source"]['search_key']['update_time']
        logger.info(f"上一次的检查点的时间戳是{last_check_timestamp}")
        logger.info(f'上一次同步clickhouse和opensearch数据的时间是{last_update_time}')
    else:
        raise Exception("没找到上一次检查点的任何信息")

    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {
                                   "bool": {
                                       "filter": [
                                           {"range": {

                                               "search_key.updated_at": {

                                                   "gte": last_check_timestamp
                                               }
                                           }}
                                       ]
                                   }
                               },
                               "sort": [
                                   {
                                       "search_key.updated_at": {
                                           "order": "asc"
                                       }
                                   }
                               ]
                           }, index=opensearch_index,
                           size=5000,
                           scroll="20m",
                           request_timeout=100)
    return results, opensearch_client


def sync_transfer_data_spacial(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas):
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
    count = 0
    # 把os中的数据一条一条拿出来
    try:
        for os_data in opensearch_datas[0]:
            updated_at = os_data["_source"]["search_key"]["updated_at"]
            if updated_at > max_timestamp:
                max_timestamp = updated_at
            insert_data = {}
            insert_data['search_key__owner'] = os_data["_source"]["search_key"]['owner']
            insert_data['search_key__repo'] = os_data["_source"]["search_key"]['repo']
            insert_data['search_key__number'] = os_data["_source"]["search_key"]['number']
            insert_data['search_key__updated_at'] = os_data["_source"]["search_key"]['updated_at']
            raw_data = os_data["_source"]["raw_data"]
            standard_data = json.dumps(raw_data, separators=(',', ':'), ensure_ascii=False)
            insert_data['timeline_raw'] = standard_data
            sql = f"INSERT INTO {table_name} (*) VALUES"
            count += 1
            if count % 5000 == 0:
                logger.info(f"已经插入的数据条数:{count}")
            result = ck.execute(sql, [insert_data])
    except Exception as e:
        # 记录最大的更新点
        # 将检查点放在这里插入
        ck_check_point(opensearch_client=opensearch_datas[1],
                       opensearch_index=opensearch_index,
                       clickhouse_table=table_name,
                       updated_at=max_timestamp)
    logger.info(f"已经插入的数据条数:{count}")
    # 将检查点放在这里插入
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=max_timestamp)
    ck.close()


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
    count = 0
    try:
        for os_data in opensearch_datas[0]:
            updated_at = os_data["_source"]["search_key"]["updated_at"]
            if updated_at > max_timestamp:
                max_timestamp = updated_at
            df = pd.json_normalize(os_data["_source"])
            dict_data = parse_data(df)
            except_fields = []
            for field in fields:
                if not dict_data.get(field):
                    except_fields.append(f'`{field}`')
                if dict_data.get(field) and fields.get(field) == 'DateTime64(3)':
                    dict_data[field] = utc_timestamp(dict_data[field])

            # 这里except_fields 里存储的就是表结构中有的fields 而数据中没有的字段
            if except_fields:
                # logger.info(f'缺失的字段列表：{except_fields}')
                except_fields = f'EXCEPT({",".join(except_fields)})'
                sql = f"INSERT INTO {table_name} (* {except_fields}) VALUES"
            else:
                sql = f"INSERT INTO {table_name} VALUES"
            count += 1
            if count % 5000 == 0:
                logger.info(f'已经插入的数据的条数为:{count}')
            try:
                result = ck.execute(sql, [dict_data])
            except KeyError as error:
                logger.error(f'插入数据发现错误 {error}')
                postgres_conn = get_postgres_conn()
                sql = '''INSERT INTO os_ck_errar(
                                    index, data) 
                                    VALUES (%s, %s);'''
                try:
                    cur = postgres_conn.cursor()
                    os_index = table_name
                    error_data = json.dumps(os_data['_source'])
                    cur.execute(sql, (os_index, error_data))
                    postgres_conn.commit()
                    cur.close()
                except (psycopg2.DatabaseError) as error:
                    logger.error(f"psycopg2.DatabaseError:{error}")

                finally:
                    if postgres_conn is not None:
                        postgres_conn.close()

    # airflow dag的中断
    except AirflowException as error:
        raise AirflowException(f'airflow interrupt {error}')
    except NotFoundError as error:
        raise NotFoundError(
            f'scroll error raise HTTP_EXCEPTIONS.get(status_code, TransportError)(opensearchpy.exceptions.NotFoundError: NotFoundError(404, "search_phase_execution_exception", "No search context found for id [631]"){error}')
    finally:
        # 将检查点放在这里插入
        ck_check_point(opensearch_client=opensearch_datas[1],
                       opensearch_index=opensearch_index,
                       clickhouse_table=table_name,
                       updated_at=max_timestamp)
        ck.close()

    logger.info(f'已经插入的数据的条数为:{count}')
    # 将检查点放在这里插入
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=max_timestamp)
    ck.close()
