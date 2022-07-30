import copy
import json
import datetime
import time
import pandas as pd
import psycopg2
from json import JSONDecodeError
from airflow import AirflowException
from clickhouse_driver.errors import ServerException
from loguru import logger
from opensearchpy import helpers
from opensearchpy.exceptions import NotFoundError
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data, get_table_structure, ck_check_point, \
    utc_timestamp, ck_check_point_repo, bulk_except, bulk_except_repo, ck_check_point_maillist
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


def sync_transfer_data_spacial_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas,
                                       owner_repo):
    transfer_type = 'github_issues_timeline_by_repo'
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
                                                        opensearch_conn_datas=opensearch_conn_datas,
                                                        clickhouse_table=table_name, repo=owner_repo,transfer_type=transfer_type)

    # 保证幂等将处于check updated_at 临界点的数据进行删除

    keep_idempotent(ck=ck,
                    search_key=owner_repo,
                    clickhouse_server_info=clickhouse_server_info,
                    table_name=table_name,
                    transfer_type=transfer_type, search_key__updated_at=opensearch_datas[2])
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
                               owner_repo, transfer_type):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])

    fields = get_table_structure(table_name=table_name, ck=ck)

    opensearch_datas = get_data_from_opensearch_by_repo(opensearch_index=opensearch_index,
                                                        opensearch_conn_datas=opensearch_conn_datas,
                                                        clickhouse_table=table_name, repo=owner_repo,
                                                        transfer_type=transfer_type)

    # 保证幂等将处于check updated_at 临界点的数据进行删除
    keep_idempotent(ck=ck,
                    search_key=owner_repo,
                    clickhouse_server_info=clickhouse_server_info,
                    table_name=table_name,
                    transfer_type=transfer_type, search_key__updated_at=opensearch_datas[2])
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
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
                raise KeyError(error)
            except ServerException as error:
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
                raise ServerException(error)
            except AttributeError as error:
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
                raise AttributeError(error)
    # airflow dag的中断
    except AirflowException as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
        raise AirflowException(f'airflow interrupt {error}')
    except NotFoundError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
        raise NotFoundError(
            f'scroll error raise HTTP_EXCEPTIONS.get(status_code, TransportError)(opensearchpy.exceptions.NotFoundError: NotFoundError(404, "search_phase_execution_exception", "No search context found for id [631]"){error}')
    except Exception as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
        raise Exception(error)
    # 处理尾部多余的数据
    try:
        if bulk_data:
            result = ck.execute(ck_sql, bulk_data)
        logger.info(f'已经插入的数据的条数为:{count}')
    except KeyError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
        raise KeyError(error)
    except ServerException as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
        raise ServerException(error)
    except AttributeError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
        raise AttributeError(error)
    except Exception as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, owner_repo, transfer_type)
        raise Exception(error)
        # 将检查点放在这里插入
    if transfer_type == "github_git_sync_by_repo":
        ck_check_point_repo(opensearch_client=opensearch_datas[1],
                            opensearch_index=opensearch_index,
                            clickhouse_table=table_name,
                            updated_at=max_timestamp, repo=owner_repo)
        time.sleep(5)
        if not if_data_eq_github(opensearch_conn_datas=opensearch_conn_datas, ck=ck, table_name=table_name,
                                 owner=owner_repo.get('owner'),
                                 repo=owner_repo.get('repo')):
            raise Exception("Inconsistent data between opensearch and clickhouse")
        else:
            logger.info("opensearch and clickhouse data are consistent")

    elif transfer_type == "maillist_init":
        ck_check_point_maillist(opensearch_client=opensearch_datas[1],
                                opensearch_index=opensearch_index,
                                clickhouse_table=table_name,
                                updated_at=max_timestamp,
                                maillist_repo=owner_repo)
        time.sleep(5)
        if not if_data_eq_maillist(opensearch_conn_datas=opensearch_conn_datas, ck=ck, table_name=table_name,
                                 project_name=owner_repo.get('project_name'),
                                 mail_list_name=owner_repo.get('mail_list_name')):
            raise Exception("Inconsistent data between opensearch and clickhouse")
        else:
            logger.info("opensearch and clickhouse data are consistent")

    ck.close()


def if_data_eq_github(opensearch_conn_datas, ck, table_name, owner, repo):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    response = opensearch_client.search(index=table_name, body={
        "size": 0,
        "track_total_hits": True,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "search_key.owner.keyword": {
                                "value": owner
                            }
                        }
                    },
                    {
                        "term": {
                            "search_key.repo.keyword": {
                                "value": repo
                            }
                        }
                    }

                ]
            }
        }
    })
    count = response["hits"]['total']['value']
    sql = f"select count() from {table_name} where search_key__owner='{owner}' and search_key__repo='{repo}'"
    # logger.info(sql)
    result = ck.execute_no_params(sql)
    logger.info(f'data count in ck {result[0][0]}')
    return count == result[0][0]


def if_data_eq_maillist(opensearch_conn_datas, ck, table_name, project_name, mail_list_name):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    response = opensearch_client.search(index=table_name, body={
        "size": 0,
        "track_total_hits": True,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "search_key.project_name.keyword": {
                                "value": project_name
                            }
                        }
                    },
                    {
                        "term": {
                            "search_key.mail_list_name.keyword": {
                                "value": mail_list_name
                            }
                        }
                    }

                ]
            }
        }
    })
    count = response["hits"]['total']['value']
    sql = f"select count() from {table_name} where search_key__project_name='{project_name}' and search_key__mail_list_name='{mail_list_name}'"
    # logger.info(sql)
    result = ck.execute_no_params(sql)
    logger.info(f'data count in ck {result[0][0]}')
    return count == result[0][0]


def  get_data_from_opensearch_by_repo(opensearch_index, opensearch_conn_datas, clickhouse_table, repo, transfer_type):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    if transfer_type == 'github_git_sync_by_repo' or transfer_type == 'github_issues_timeline_by_repo':
        body = {
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
        }
    else:
        body = {
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
                                "search_key.project_name.keyword": {
                                    "value": repo.get('project_name')
                                }
                            }
                        },
                        {
                            "term": {
                                "search_key.mail_list_name.keyword": {
                                    "value": repo.get('mail_list_name')
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
        }
    # 获取上一次的检查点
    response = opensearch_client.search(index=OPENSEARCH_INDEX_CHECK_SYNC_DATA, body=body)
    if response['hits']['hits']:
        last_check_timestamp = response['hits']['hits'][0]["_source"]["os_ck"]["last_data"]["updated_at"]
        last_update_time = response['hits']['hits'][0]["_source"]['search_key']['update_time']
        logger.info(f"上一次的检查点的时间戳是{last_check_timestamp}")
        logger.info(f'上一次同步clickhouse和opensearch数据的时间是{last_update_time}')
    else:
        raise Exception("没找到上一次检查点的任何信息")
    if transfer_type == 'github_git_sync_by_repo':
        scan_body = {
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
        }
    else:
        scan_body = {
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
                            "search_key.project_name.keyword": {
                                "value": repo.get('project_name')
                            }
                        }}, {"term": {
                            "search_key.mail_list_name.keyword": {
                                "value": repo.get('mail_list_name')
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
        }
    results = helpers.scan(client=opensearch_client,
                           query=scan_body, index=opensearch_index,
                           size=5000,
                           scroll="20m",
                           request_timeout=100,
                           preserve_order=True)
    return results, opensearch_client, last_check_timestamp


def keep_idempotent(ck, search_key, clickhouse_server_info, table_name, transfer_type, search_key__updated_at):
    # print(f"{transfer_type}-------------------------------------------------------------")
    # 获取clickhouse集群节点信息
    get_clusters_sql = f"select host_name from system.clusters where cluster = '{clickhouse_server_info['CLUSTER_NAME']}'"
    # print(get_clusters_sql)
    clusters_info = ck.execute_no_params(get_clusters_sql)
    delect_data_sql = ''
    check_if_done_sql = ''
    # 保证幂等
    import time
    # 获得当前时间时间戳
    now = int(time.time())
    # logger.info(type)
    # raise Exception("停止一下")
    if transfer_type == 'github_git_sync_by_repo' or transfer_type == 'github_issues_timeline_by_repo':
        owner = search_key.get('owner')
        repo = search_key.get('repo')
        delect_data_sql = f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE {now}={now} and search_key__owner = '{owner}' and search_key__repo = '{repo}' and search_key__updated_at = {search_key__updated_at}"
        check_if_done_sql = f"select is_done from system.mutations m WHERE `table` = '{table_name}_local' and command = 'DELETE WHERE ({now} = {now}) AND (search_key__owner = \\'{owner}\\') AND (search_key__repo = \\'{repo}\\') AND (search_key__updated_at = {search_key__updated_at})'"

    elif transfer_type == 'maillist_init':
        project_name = search_key.get('project_name')
        mail_list_name = search_key.get('mail_list_name')
        delect_data_sql = f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE {now} = {now} and search_key__project_name = '{project_name}' and search_key__mail_list_name = '{mail_list_name}'"
        check_if_done_sql = f"select is_done from system.mutations m WHERE ` ` = '{table_name}_local' and command = 'DELETE WHERE ({now} = {now}) AND (search_key__project_name = \\'{project_name}\\') AND (search_key__mail_list_name = \\'{mail_list_name}\\') AND (search_key__updated_at = {search_key__updated_at})'"
    ck.execute_no_params(delect_data_sql)
    # logger.info("将同owner和repo的老数据进行删除")

    time.sleep(1)
    logger.info("等待1秒，等待数据删除成功")
    wait_count = 1
    while 1:
        delete_flag = 1
        for cluster_host_name in clusters_info:
            logger.info(f"clickhouse 节点：{cluster_host_name[0]}")
            all_ck = CKServer(host=f'{cluster_host_name[0]}',
                              port=9000,
                              user=clickhouse_server_info["USER"],
                              password=clickhouse_server_info["PASSWD"],
                              database=clickhouse_server_info["DATABASE"])
            result = all_ck.execute_no_params(check_if_done_sql)
            print(result)
            if result and result[0][0] == 1:
                logger.info(f"{cluster_host_name[0]} 上已经将数据删除成功删除")
            else:
                delete_flag = 0
                logger.info(f"{cluster_host_name[0]} 上没有将数据删除成功删除")
        if delete_flag == 1:
            logger.info("所有节点上的相关数据已经完全删除")
            time.sleep(30)
            break
        time.sleep(1)
        wait_count += 1
        if wait_count == 60:
            raise Exception("等待时间过长，删除时间超时,无法保证幂等")
