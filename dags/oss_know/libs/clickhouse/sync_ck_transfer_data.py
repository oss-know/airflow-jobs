import copy
import json
import datetime
from json import JSONDecodeError

import pandas as pd
import psycopg2
from airflow import AirflowException
from clickhouse_driver.errors import ServerException
from loguru import logger
from opensearchpy import helpers
from opensearchpy.exceptions import NotFoundError
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data, get_table_structure, ck_check_point, \
    utc_timestamp, ck_check_point_repo, bulk_except
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
                           request_timeout=100,
                           preserve_order=True)
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


# def bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, repo):
#     start_updated_at = bulk_data[0]['search_key__updated_at']
#     end_updated_at = bulk_data[-1]['search_key__updated_at']
#     ck_check_point_repo(opensearch_client=opensearch_datas[1],
#                         opensearch_index=opensearch_index,
#                         clickhouse_table=table_name,
#                         updated_at=start_updated_at, repo=repo)
#     logger.error(f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')


def sync_transfer_data_spacial_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas,owner_repo):
    bulk_datas = []
    template = {
        "search_key": {
            "owner": "",
            "repo": "",
            "number": 0,
            "updated_at": 0
        },
        "raw_data": {
            "timeline_raw": ""
        }
    }
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    opensearch_datas = get_data_from_opensearch_by_repo(opensearch_index=opensearch_index,
                                                        opensearch_conn_datas=opensearch_conn_datas,clickhouse_table=table_name, repo=owner_repo)

    owner = owner_repo.get('owner')
    repo = owner_repo.get('repo')

    # 保证幂等将处于check updated_at 临界点的数据进行删除
    ck.execute_no_params(
        f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE search_key__owner = '{owner}' and search_key__repo = '{repo}' and search_key__updated_at = {opensearch_datas[2]}")
    logger.info("保证幂等将处于check updated_at 临界点的数据进行删除")
    max_timestamp = 0
    count = 0
    # 把os中的数据一条一条拿出来
    try:
        for os_data in opensearch_datas[0]:
            updated_at = os_data["_source"]["search_key"]["updated_at"]
            if updated_at > max_timestamp:
                max_timestamp = updated_at
            insert_data = copy.deepcopy(template)
            insert_data['search_key__owner'] = os_data["_source"]["search_key"]['owner']
            insert_data['search_key__repo'] = os_data["_source"]["search_key"]['repo']
            insert_data['search_key__number'] = os_data["_source"]["search_key"]['number']
            insert_data['search_key__updated_at'] = os_data["_source"]["search_key"]['updated_at']
            raw_data = os_data["_source"]["raw_data"]
            standard_data = json.dumps(raw_data, separators=(',', ':'), ensure_ascii=False)
            insert_data['timeline_raw'] = standard_data
            bulk_datas.append(insert_data)
            sql = f"INSERT INTO {table_name} (*) VALUES"
            count += 1
            if count % 50000 == 0:
                result = ck.execute(sql, bulk_datas)
                logger.info(f"已经插入的数据条数:{count}")
                bulk_datas.clear()
                max_timestamp = 0
        if bulk_datas:
            result = ck.execute(sql, bulk_datas)
        logger.info(f"已经插入的数据条数:{count}")
    except Exception as error:
        bulk_except(bulk_datas, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise Exception(error)

    # 将检查点放在这里插入
    ck_check_point_repo(opensearch_client=opensearch_datas[1],
                        opensearch_index=opensearch_index,
                        clickhouse_table=table_name,
                        updated_at=max_timestamp, repo=owner_repo)
    ck.close()


def sync_transfer_data_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, template,
                               owner_repo):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])

    fields = get_table_structure(table_name=table_name, ck=ck)
    opensearch_datas = get_data_from_opensearch_by_repo(opensearch_index=opensearch_index,
                                                        opensearch_conn_datas=opensearch_conn_datas,clickhouse_table=table_name, repo=owner_repo)

    owner = owner_repo.get('owner')
    repo = owner_repo.get('repo')

    # 保证幂等将处于check updated_at 临界点的数据进行删除
    ck.execute_no_params(
        f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE search_key__owner = '{owner}' and search_key__repo = '{repo}' and search_key__updated_at = {opensearch_datas[2]}")
    logger.info("保证幂等将处于check updated_at 临界点的数据进行删除")
    max_timestamp = 0
    count = 0
    bulk_data = []
    try:
        for os_data in opensearch_datas[0]:
            updated_at = os_data["_source"]["search_key"]["updated_at"]
            if updated_at > max_timestamp:
                max_timestamp = updated_at
            df_data = os_data["_source"]

            df = pd.json_normalize(df_data)
            dict_data = parse_data(df, template)
            try:
                dict_dict = json.loads(json.dumps(dict_data))
            except JSONDecodeError as error:
                logger.error(error)
                continue
            for field in fields:
                if dict_dict.get(field) and fields.get(field) == 'DateTime64(3)':
                    dict_dict[field] = datetime.datetime.strptime(dict_dict[field], '%Y-%m-%dT%H:%M:%SZ')
                elif fields.get(field) == 'String':
                    try:
                        dict_dict[field].encode('utf-8')
                    except UnicodeEncodeError as error:
                        dict_dict[field] = dict_dict[field].encode('unicode-escape').decode('utf-8')
            bulk_data.append(dict_dict)
            ck_sql = f"INSERT INTO {table_name} VALUES"
            try:
                # result = ck.execute(ck_sql, [dict_data])
                count += 1
                if count % 20000 == 0:
                    result = ck.execute(ck_sql, bulk_data)
                    bulk_data.clear()
                    max_timestamp = 0
                    logger.info(f'已经插入的数据的条数为:{count}')
            except KeyError as error:
                bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
                raise KeyError(error)
            except ServerException as error:
                bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
                raise ServerException(error)
            except AttributeError as error:
                bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
                raise AttributeError(error)
    # airflow dag的中断
    except AirflowException as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise AirflowException(f'airflow interrupt {error}')
    except NotFoundError as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise NotFoundError(
            f'scroll error raise HTTP_EXCEPTIONS.get(status_code, TransportError)(opensearchpy.exceptions.NotFoundError: NotFoundError(404, "search_phase_execution_exception", "No search context found for id [631]"){error}')
    except Exception as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise Exception(error)
    # 处理尾部多余的数据
    try:
        if bulk_data:
            result = ck.execute(ck_sql, bulk_data)
        logger.info(f'已经插入的数据的条数为:{count}')
    except KeyError as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise KeyError(error)
    except ServerException as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise ServerException(error)
    except AttributeError as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise AttributeError(error)
    except Exception as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo)
        raise Exception(error)
    # 将检查点放在这里插入
    ck_check_point_repo(opensearch_client=opensearch_datas[1],
                        opensearch_index=opensearch_index,
                        clickhouse_table=table_name,
                        updated_at=max_timestamp, repo=owner_repo)
    ck.close()


def get_data_from_opensearch_by_repo(opensearch_index, opensearch_conn_datas, clickhouse_table, repo):
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
                    },
                    {
                        "term": {
                            "search_key.owner.keyword": {
                                "value": repo.get('owner')
                            }
                        }
                    },
                    {
                        "term": {
                            "search_key.repo.keyword": {
                                "value": repo.get('repo')
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
                                       ], "must": [
                                           {"term": {
                                               "search_key.owner.keyword": {
                                                   "value": repo.get('owner')
                                               }
                                           }}, {"term": {
                                               "search_key.repo.keyword": {
                                                   "value": repo.get('repo')
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
                           request_timeout=100,
                           preserve_order=True)
    return results, opensearch_client, last_check_timestamp
