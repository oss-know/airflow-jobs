import datetime
import json
import time
import numpy
import pandas as pd
import psycopg2
import warnings
import copy
from clickhouse_driver.columns.exceptions import StructPackException
from json import JSONDecodeError
from clickhouse_driver.errors import ServerException
from loguru import logger
from opensearchpy import helpers
from airflow.exceptions import AirflowException
from opensearchpy.exceptions import NotFoundError
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_RAW_DATA
from oss_know.libs.util.airflow import get_postgres_conn
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


def ck_check_point_repo(opensearch_client, opensearch_index, clickhouse_table, updated_at, repo):
    now_time = datetime.datetime.now()
    check_info = {
        "search_key": {
            "type": "os_ck",
            "update_time": now_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "update_timestamp": now_time.timestamp(),
            "opensearch_index": opensearch_index,
            "clickhouse_table": clickhouse_table,
            "owner": repo.get("owner"),
            "repo": repo.get("repo")
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
    elif isinstance(row, numpy.float64):
        row = float(row)
    else:
        pass
    return row


# 特殊情况 by repo
def transfer_data_special_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, owner_repo):
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
    opensearch_datas = get_data_from_opensearch_by_repo(index=opensearch_index,
                                                        opensearch_conn_datas=opensearch_conn_datas, repo=owner_repo)
    owner = owner_repo.get('owner')
    repo  = owner_repo.get('repo')
    # 保证幂等
    ck.execute_no_params(
        f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE search_key__owner = '{owner}' and search_key__repo = '{repo}'")
    logger.info("将同owner和repo的老数据进行删除")
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


# 特殊情况
def transfer_data_special(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas):
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
    opensearch_datas = get_data_from_opensearch(index=opensearch_index,
                                                opensearch_conn_datas=opensearch_conn_datas)
    max_timestamp = 0
    count = 0
    # 把os中的数据一条一条拿出来
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
    if bulk_datas:
        result = ck.execute(sql, bulk_datas)
    logger.info(f"已经插入的数据条数:{count}")

    # 将检查点放在这里插入
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=max_timestamp)
    ck.close()


warnings.filterwarnings('ignore')


def transfer_data(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, template):
    # if opensearch_index == 'maillists' or opensearch_index == 'maillists_enriched':
    #     settings = {'strings_encoding': 'unicode_escape'}
    # else:
    #     settings = {'strings_encoding': 'utf-8'}
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    fields = get_table_structure(table_name=table_name, ck=ck)
    opensearch_datas = get_data_from_opensearch(index=opensearch_index,
                                                opensearch_conn_datas=opensearch_conn_datas)
    max_timestamp = 0
    count = 0
    bulk_data = []
    try:
        for os_data in opensearch_datas[0]:
            updated_at = os_data["_source"]["search_key"]["updated_at"]
            # # 特殊情况要记得删掉
            # if isinstance(updated_at,float):
            #     continue
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
            # except_fields = []
            for field in fields:
                # if not dict_data.get(field):
                #     except_fields.append(f'`{field}`')
                if dict_dict.get(field) and fields.get(field) == 'DateTime64(3)':
                    # dict_data[field] = utc_timestamp(dict_data[field])
                    # logger.info(f'dict_data:{dict_data[field]} type:{type(dict_data[field])}')
                    dict_dict[field] = datetime.datetime.strptime(dict_dict[field], '%Y-%m-%dT%H:%M:%SZ')
                    # logger.info(f'dict_data--datetime-type:{dict_data[field]} type:{type(dict_data[field])}')
                elif fields.get(field) == 'String':
                    try:
                        dict_dict[field].encode('utf-8')
                    except UnicodeEncodeError as error:
                        dict_dict[field] = dict_dict[field].encode('unicode-escape').decode('utf-8')
            bulk_data.append(dict_dict)
            # 这里except_fields 里存储的就是表结构中有的fields 而数据中没有的字段
            # if except_fields:
            #     # logger.info(f'缺失的字段列表：{except_fields}')
            #     except_fields = f'EXCEPT({",".join(except_fields)})'
            #     sql = f"INSERT INTO {table_name} (* {except_fields}) VALUES"
            # else:
            #     sql = f"INSERT INTO {table_name} VALUES"
            ck_sql = f"INSERT INTO {table_name} VALUES"
            try:
                # result = ck.execute(ck_sql, [dict_data])
                count += 1
                if count % 20000 == 0:
                    result = ck.execute(ck_sql, bulk_data)
                    bulk_data.clear()
                    logger.info(f'已经插入的数据的条数为:{count}')
                    # print(bulk_data)
                    # raise Exception("运行一就停")
                # ck.execute_use_setting(sql=sql, params=[dict_data], settings=settings)
            except KeyError as error:
                logger.error(f'插入数据发现错误 {error}')
                logger.error(f'出现问题的数据是{dict_dict}')
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
            except ServerException as error:
                print(f'----------------------------Server')
                logger.error(f'插入数据发现错误 {error}')
                logger.error(f'出现问题的数据是{dict_dict}')
                # postgres_conn = get_postgres_conn()
                # sql = '''INSERT INTO os_ck_errar(
                #                                     index, data)
                #                                     VALUES (%s, %s);'''
                # try:
                #     cur = postgres_conn.cursor()
                #     os_index = table_name
                #     error_data = json.dumps(os_data['_source'])
                #     cur.execute(sql, (os_index, error_data))
                #     postgres_conn.commit()
                #     cur.close()
                # except (psycopg2.DatabaseError) as error:
                #     logger.error(f"psycopg2.DatabaseError:{error}")
                #
                # finally:
                #     if postgres_conn is not None:
                #         postgres_conn.close()
            except UnicodeEncodeError as error:
                logger.error(f'插入数据发现错误 {error}')
                logger.error(f'出现问题的数据是{dict_dict}')
                logger.error(f'舍弃这条数据')
                # logger.error(f'使用unicode进行编码:{json.loads(json.dumps(dict_dict))}')
                # settings = {'strings_encoding': 'gbk'}
                # try:
                #     ck.execute_use_setting(sql=sql, params=[dict_data], settings=settings)
                #     count += 1
                #     if count % 5000 == 0:
                #         logger.info(f'已经插入的数据的条数为:{count}')
                # except UnicodeEncodeError as error:
                #     logger.error(f'使用Unicode编码还是出现问题')
                #     logger.error(f'插入数据发现错误 {error}')
                #     logger.error(f'出现问题的数据是{dict_data}')
            except AttributeError as error:
                logger.error(f'插入数据发现错误 {error}')
                logger.error(f'出现问题的数据是{json.dumps(dict_dict)}')
                raise AttributeError(error)


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
    if bulk_data:
        result = ck.execute(ck_sql, bulk_data)
    logger.info(f'已经插入的数据的条数为:{count}')
    # 将检查点放在这里插入
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=max_timestamp)
    ck.close()


def bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name, repo):
    start_updated_at = bulk_data[0]['search_key__updated_at']
    end_updated_at = bulk_data[-1]['search_key__updated_at']
    ck_check_point_repo(opensearch_client=opensearch_datas[1],
                        opensearch_index=opensearch_index,
                        clickhouse_table=table_name,
                        updated_at=start_updated_at, repo=repo)
    logger.error(f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')


def transfer_data_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, template, owner_repo):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    owner = owner_repo.get('owner')
    repo = owner_repo.get('repo')
    # 保证幂等
    ck.execute_no_params(
        f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE search_key__owner = '{owner}' and search_key__repo = '{repo}'")
    logger.info("将同owner和repo的老数据进行删除")
    fields = get_table_structure(table_name=table_name, ck=ck)
    opensearch_datas = get_data_from_opensearch_by_repo(index=opensearch_index,
                                                        opensearch_conn_datas=opensearch_conn_datas, repo=owner_repo)
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


def get_data_from_opensearch(index, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {"match_all": {}},
                               "sort": [
                                   {
                                       "search_key.updated_at": {
                                           "order": "asc"
                                       }
                                   }
                               ]
                           },
                           index=index,
                           size=5000,
                           scroll="40m",
                           request_timeout=100,
                           preserve_order=True)
    return results, opensearch_client


def get_data_from_opensearch_by_repo(index, opensearch_conn_datas, repo):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {
                                   "bool": {
                                       "must": [
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
                                       "search_key.updated_at": {
                                           "order": "asc"
                                       }
                                   }
                               ]
                           },
                           index=index,
                           size=5000,
                           scroll="40m",
                           request_timeout=100,
                           preserve_order=True)
    return results, opensearch_client


def parse_data(df, temp):
    # 这个是最终插入ck的数据字典
    dict_data = copy.deepcopy(temp)
    for index, row in df.iloc[0].iteritems():
        # 去除以raw_data开头的字段
        if index.startswith(CLICKHOUSE_RAW_DATA):
            index = index[9:]
        index = index.replace('.', '__')
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


def parse_data_init(df):
    dict_data = {}
    for index, row in df.iloc[0].iteritems():
        # 去除以raw_data开头的字段
        if index.startswith(CLICKHOUSE_RAW_DATA):
            index = index[9:]
        index = index.replace('.', '__')
        # 只要是空的就跳过
        # if not row:
        #     continue
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
