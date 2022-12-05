import datetime
import json
import time
import numpy
import random
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


def ck_check_point_maillist(opensearch_client, opensearch_index, clickhouse_table, updated_at, maillist_repo):
    now_time = datetime.datetime.now()
    check_info = {
        "search_key": {
            "type": "os_ck",
            "update_time": now_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "update_timestamp": now_time.timestamp(),
            "opensearch_index": opensearch_index,
            "clickhouse_table": clickhouse_table,
            "project_name": maillist_repo.get("project_name"),
            "mail_list_name": maillist_repo.get("mail_list_name")
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
    return row


# 特殊情况 by repo
def transfer_data_special_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas,
                                  search_key):
    bulk_datas = []
    template = {

        "deleted":0
    }
    template["ck_data_insert_at"] = int(round(time.time() * 1000))
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])

    transfer_type = 'github_issues_timeline_by_repo'
    owner = search_key["owner"]
    repo = search_key["repo"]
    # 判断项目是否在ck中存在
    if_null_sql = f"select count() from {table_name} where search_key__owner='{owner}' and search_key__repo='{repo}'"
    if_null_result = ck.execute_no_params(if_null_sql)
    if if_null_result[0][0] != 0:
        keep_idempotent(ck=ck, search_key=search_key, clickhouse_server_info=clickhouse_server_info,
                        table_name=table_name, transfer_type="github_git_init_by_repo")
    else:
        logger.info("No data in CK")
    logger.info("github_git_init_by_repo------------------------")
    # keep_idempotent(ck=ck, clickhouse_server_info=clickhouse_server_info, search_key=search_key,
    #                 transfer_type=transfer_type, table_name=table_name)
    opensearch_datas = get_data_from_opensearch_by_repo(index=opensearch_index,
                                                        opensearch_conn_datas=opensearch_conn_datas, repo=search_key)
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
            insert_data['search_key__event'] = os_data["_source"]["search_key"]['event']
            insert_data['search_key__updated_at'] = os_data["_source"]["search_key"]['updated_at']
            insert_data['search_key__uuid'] = os_data["_source"]["search_key"]['uuid']
            insert_data['search_key__if_sync'] = os_data["_source"]["search_key"]['if_sync']
            raw_data = os_data["_source"]["raw_data"]
            standard_data = json.dumps(raw_data, separators=(',', ':'), ensure_ascii=False)
            insert_data['timeline_raw'] = standard_data
            bulk_datas.append(insert_data)
            sql = f"INSERT INTO {table_name} (*) VALUES"
            count += 1
            # print(insert_data)
            # return
            if count % 50000 == 0:
                result = ck.execute(sql, bulk_datas)
                logger.info(f"已经插入的数据条数:{count}")
                bulk_datas.clear()
                max_timestamp = 0
        if bulk_datas:
            result = ck.execute(sql, bulk_datas)
        logger.info(f"已经插入的数据条数:{count}")
    except Exception as error:
        # bulk_except_repo(bulk_datas, opensearch_datas, opensearch_index, table_name, search_key,
        #                  transfer_type=transfer_type)
        logger.error(error)
        logger.info()
        raise Exception(error)

    # 将检查点放在这里插入
    ck_check_point_repo(opensearch_client=opensearch_datas[1],
                        opensearch_index=opensearch_index,
                        clickhouse_table=table_name,
                        updated_at=max_timestamp, repo=search_key)
    time.sleep(10)
    if not if_data_eq_github(count=count, ck=ck, table_name=table_name, owner=search_key.get('owner'),
                             repo=search_key.get('repo')):
        raise Exception("Inconsistent data between opensearch and clickhouse")
    else:
        logger.info("opensearch and clickhouse data are consistent")
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
            if updated_at > max_timestamp:
                max_timestamp = updated_at
            df_data = os_data["_source"]

            df = pd.json_normalize(df_data)
            template["ck_data_insert_at"] = int(round(time.time() * 1000))
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
                bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
                raise KeyError(error)
            except ServerException as error:
                bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
                raise ServerException(error)
            except AttributeError as error:
                bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
                raise AttributeError(error)
    # airflow dag的中断
    except AirflowException as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
        raise AirflowException(f'airflow interrupt {error}')
    except NotFoundError as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
        raise NotFoundError(
            f'scroll error raise HTTP_EXCEPTIONS.get(status_code, TransportError)(opensearchpy.exceptions.NotFoundError: NotFoundError(404, "search_phase_execution_exception", "No search context found for id [631]"){error}')
    except Exception as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
        raise Exception(error)
    # 处理尾部多余的数据
    try:
        if bulk_data:
            result = ck.execute(ck_sql, bulk_data)
        logger.info(f'已经插入的数据的条数为:{count}')
    except KeyError as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
        raise KeyError(error)
    except ServerException as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
        raise ServerException(error)
    except AttributeError as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
        raise AttributeError(error)
    except Exception as error:
        bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name)
        raise Exception(error)
    # 将检查点放在这里插入
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=max_timestamp)
    ck.close()


# check_structure
def transfer_data_check_structure(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas,
                                  template):
    try:
        ck = CKServer(host=clickhouse_server_info["HOST"],
                      port=clickhouse_server_info["PORT"],
                      user=clickhouse_server_info["USER"],
                      password=clickhouse_server_info["PASSWD"],
                      database=clickhouse_server_info["DATABASE"])
        fields = get_table_structure(table_name=table_name, ck=ck)
        opensearch_datas = get_data_from_opensearch(index=opensearch_index,
                                                    opensearch_conn_datas=opensearch_conn_datas)
        max_timestamp = 0
        flag = 0
        mistake_structure = []
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
                if dict_dict.get(field) and fields.get(field) == 'String':
                    try:
                        dict_dict[field] = datetime.datetime.strptime(dict_dict[field], '%Y-%m-%dT%H:%M:%SZ')
                        flag = 1
                        mistake_structure.append(field)

                    except ValueError:
                        pass
        if flag:
            raise Exception(f"数据声明为字符串类型但是数据为日期类型,字段为{list(set(mistake_structure))}")
    except AirflowException as error:
        if flag:
            raise Exception(f"数据声明为字符串类型但是数据为日期类型,字段为{list(set(mistake_structure))}")
        raise AirflowException(f'airflow interrupt {error}')


def bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type):
    start_updated_at = bulk_data[0]['search_key__updated_at']
    end_updated_at = bulk_data[-1]['search_key__updated_at']
    if transfer_type == "github_git_init_by_repo" or transfer_type == 'github_issues_timeline_by_repo':
        ck_check_point_repo(opensearch_client=opensearch_datas[1],
                            opensearch_index=opensearch_index,
                            clickhouse_table=table_name,
                            updated_at=start_updated_at, repo=search_key)
    elif transfer_type == "maillist_init":
        ck_check_point_maillist(opensearch_client=opensearch_datas[1],
                                opensearch_index=opensearch_index,
                                clickhouse_table=table_name,
                                updated_at=start_updated_at,
                                maillist_repo=search_key)
    logger.error(f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')


def bulk_except_maillist(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo):
    start_updated_at = bulk_data[0]['search_key__updated_at']
    end_updated_at = bulk_data[-1]['search_key__updated_at']
    ck_check_point_maillist(opensearch_client=opensearch_datas[1],
                            opensearch_index=opensearch_index,
                            clickhouse_table=table_name,
                            updated_at=start_updated_at, maillist_repo=maillist_repo)
    logger.error(f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')


def bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name):
    start_updated_at = bulk_data[0]['search_key__updated_at']
    end_updated_at = bulk_data[-1]['search_key__updated_at']
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=start_updated_at)
    logger.error(f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')


# 保证插入数据的幂等性
def keep_idempotent(ck, search_key, clickhouse_server_info, table_name, transfer_type):
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
    if transfer_type == 'github_git_init_by_repo' or transfer_type == 'github_issues_timeline_by_repo':
        owner = search_key.get('owner')
        repo = search_key.get('repo')
        delect_data_sql = f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE {now}={now} and search_key__owner = '{owner}' and search_key__repo = '{repo}'"
        check_if_done_sql = f"select is_done from system.mutations m WHERE `table` = '{table_name}_local' and command = 'DELETE WHERE ({now} = {now}) AND (search_key__owner = \\'{owner}\\') AND (search_key__repo = \\'{repo}\\')'"
    elif transfer_type == 'maillist_init':
        project_name = search_key.get('project_name')
        mail_list_name = search_key.get('mail_list_name')
        delect_data_sql = f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} DELETE WHERE {now} = {now} and search_key__project_name = '{project_name}' and search_key__mail_list_name = '{mail_list_name}'"
        check_if_done_sql = f"select is_done from system.mutations m WHERE `table` = '{table_name}_local' and command = 'DELETE WHERE ({now} = {now}) AND (search_key__project_name = \\'{project_name}\\') AND (search_key__mail_list_name = \\'{mail_list_name}\\')'"
    ck.execute_no_params(delect_data_sql)
    logger.info("将同owner和repo的老数据进行删除")

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
            logger.info("所有节点上的相关数据已经删除")
            time.sleep(60)
            break
        time.sleep(1)
        wait_count += 1
        if wait_count == 60:
            raise Exception("等待时间过长，删除时间超时,无法保证幂等")


def transfer_data_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, template,
                          search_key, transfer_type):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])

    if transfer_type == "github_git_init_by_repo":
        search_key_owner = search_key['owner']
        search_key_repo = search_key['repo']
        # 判断项目是否在ck中存在
        if_null_sql = f"select count() from {table_name} where search_key__owner='{search_key_owner}' and search_key__repo='{search_key_repo}'"
        if_null_result = ck.execute_no_params(if_null_sql)
        if if_null_result[0][0] != 0:
            keep_idempotent(ck=ck, search_key=search_key, clickhouse_server_info=clickhouse_server_info,
                            table_name=table_name, transfer_type="github_git_init_by_repo")
        else:
            logger.info("No data in CK")
        logger.info("github_git_init_by_repo------------------------")
        opensearch_datas = get_data_from_opensearch_by_repo(index=opensearch_index,
                                                            opensearch_conn_datas=opensearch_conn_datas,
                                                            repo=search_key)
    elif transfer_type == "maillist_init":
        search_key_project_name = search_key['project_name']
        search_key_mail_list_name = search_key['mail_list_name']
        if_null_sql = f"select count() from {table_name} where search_key__project_name='{search_key_project_name}' and search_key__mail_list_name='{search_key_mail_list_name}'"
        if_null_result = ck.execute_no_params(if_null_sql)
        if not if_null_result:
            keep_idempotent(ck=ck, search_key=search_key, clickhouse_server_info=clickhouse_server_info,
                            table_name=table_name, transfer_type="maillist_init")
        logger.info("maillist------------------------")
        opensearch_datas = get_data_from_opensearch_maillist(index=opensearch_index,
                                                             opensearch_conn_datas=opensearch_conn_datas,
                                                             maillist_repo=search_key)
    fields = get_table_structure(table_name=table_name, ck=ck)
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
            template["ck_data_insert_at"] = int(round(time.time() * 1000))
            template["deleted"] = 0
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
                    # random.shuffle(bulk_data)
                    result = ck.execute(ck_sql, bulk_data)
                    bulk_data.clear()
                    max_timestamp = 0
                    logger.info(f'已经插入的数据的条数为:{count}')
            except KeyError as error:
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
                raise KeyError(error)
            except ServerException as error:
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
                raise ServerException(error)
            except AttributeError as error:
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
                raise AttributeError(error)
    # airflow dag的中断
    except AirflowException as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
        raise AirflowException(f'airflow interrupt {error}')
    except NotFoundError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
        raise NotFoundError(
            f'scroll error raise HTTP_EXCEPTIONS.get(status_code, TransportError)(opensearchpy.exceptions.NotFoundError: NotFoundError(404, "search_phase_execution_exception", "No search context found for id [631]"){error}')
    except Exception as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
        raise Exception(error)
    # 处理尾部多余的数据
    try:
        if bulk_data:
            # random.shuffle(bulk_data)
            result = ck.execute(ck_sql, bulk_data)
        logger.info(f'已经插入的数据的条数为:{count}')
    except KeyError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
        raise KeyError(error)
    except ServerException as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
        raise ServerException(error)
    except AttributeError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
        raise AttributeError(error)
    except Exception as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
        raise Exception(error)
    # 将检查点放在这里插入
    if transfer_type == "github_git_init_by_repo":
        ck_check_point_repo(opensearch_client=opensearch_datas[1],
                            opensearch_index=opensearch_index,
                            clickhouse_table=table_name,
                            updated_at=max_timestamp, repo=search_key)
        # time.sleep(10)
        # if not if_data_eq_github(count=count, ck=ck, table_name=table_name, owner=search_key.get('owner'),
        #                          repo=search_key.get('repo')):
        #     raise Exception("Inconsistent data between opensearch and clickhouse")
        # else:
        #     logger.info("opensearch and clickhouse data are consistent")

    elif transfer_type == "maillist_init":
        ck_check_point_maillist(opensearch_client=opensearch_datas[1],
                                opensearch_index=opensearch_index,
                                clickhouse_table=table_name,
                                updated_at=max_timestamp,
                                maillist_repo=search_key)
        # time.sleep(10)
        # if not if_data_eq_maillist(count=count, ck=ck, table_name=table_name,
        #                            project_name=search_key.get('project_name'),
        #                            mail_list_name=search_key.get('mail_list_name')):
        #     raise Exception("Inconsistent data between opensearch and clickhouse")
        # else:
        #     logger.info("opensearch and clickhouse data are consistent")

    ck.close()


def transfer_data_maillist(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, template,
                           maillist_repo):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    keep_idempotent(ck=ck, clickhouse_server_info=clickhouse_server_info, search_key=maillist_repo,
                    table_name=table_name, transfer_type='maillist_init')
    fields = get_table_structure(table_name=table_name, ck=ck)
    opensearch_datas = get_data_from_opensearch_maillist(index=opensearch_index,
                                                         opensearch_conn_datas=opensearch_conn_datas,
                                                         maillist_repo=maillist_repo)
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
            template["ck_data_insert_at"] = int(round(time.time() * 1000))
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
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
                raise KeyError(error)
            except ServerException as error:
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
                raise ServerException(error)
            except AttributeError as error:
                bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
                raise AttributeError(error)
    # airflow dag的中断
    except AirflowException as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
        raise AirflowException(f'airflow interrupt {error}')
    except NotFoundError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
        raise NotFoundError(
            f'scroll error raise HTTP_EXCEPTIONS.get(status_code, TransportError)(opensearchpy.exceptions.NotFoundError: NotFoundError(404, "search_phase_execution_exception", "No search context found for id [631]"){error}')
    except Exception as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
        raise Exception(error)
    # 处理尾部多余的数据
    try:
        if bulk_data:
            result = ck.execute(ck_sql, bulk_data)
        logger.info(f'已经插入的数据的条数为:{count}')
    except KeyError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
        raise KeyError(error)
    except ServerException as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
        raise ServerException(error)
    except AttributeError as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
        raise AttributeError(error)
    except Exception as error:
        bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo)
        raise Exception(error)
    # 将检查点放在这里插入
    ck_check_point_repo(opensearch_client=opensearch_datas[1],
                        opensearch_index=opensearch_index,
                        clickhouse_table=table_name,
                        updated_at=max_timestamp, repo=maillist_repo)

    ck.close()


# 判断opensearch和ck数据是否一致

def if_data_eq_github(count, ck, table_name, owner, repo):
    sql = f"select count() from {table_name} where search_key__owner='{owner}' and search_key__repo='{repo}'"
    # logger.info(sql)
    result = ck.execute_no_params(sql)
    logger.info(f'data count in ck {result[0][0]}')
    return count == result[0][0]


def if_data_eq_maillist(count, ck, table_name, project_name, mail_list_name):
    sql = f"select count() from {table_name} where search_key__project_name='{project_name}' and search_key__mail_list_name='{mail_list_name}'"
    # logger.info(sql)
    result = ck.execute_no_params(sql)
    # logger.info(result)
    logger.info(f'data count in ck {result[0][0]}')
    return count == result[0][0]


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


def get_data_from_opensearch_maillist(index, opensearch_conn_datas, maillist_repo):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {
                                   "bool": {
                                       "must": [
                                           {
                                               "term": {
                                                   "search_key.project_name.keyword": {
                                                       "value": maillist_repo.get('project_name')
                                                   }
                                               }
                                           },
                                           {
                                               "term": {
                                                   "search_key.mail_list_name.keyword": {
                                                       "value": maillist_repo.get('mail_list_name')
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




#def parse_data(df, temp):
#     # 这个是最终插入ck的数据字典
#     dict_data = copy.deepcopy(temp)
#     for index, row in df.iloc[0].iteritems():
#         # 去除以raw_data开头的字段
#         if index.startswith(CLICKHOUSE_RAW_DATA):
#             index = index[9:]
#         index = index.replace('.', '__')
#         # 只要是空的就跳过
#         if not row:
#             continue
#         # 第一步的转化
#         row = alter_data_type(row)
#         # # 这里的字符串都转换成在ck中能够使用函数解析json的标准格式
#         # if isinstance(row, str):
#         #     row.replace(": ", ":")
#         # 解决嵌套array
#         if isinstance(row, list):
#             # 数组中是字典
#             if isinstance(row[0], dict):
#                 for key in row[0]:
#                     data_name = f'{index}.{key}'
#                     dict_data[data_name] = []
#                 for data in row:
#                     for key in data:
#                         filter_data = alter_data_type(data.get(key))
#                         dict_data.get(f'{index}.{key}').append(filter_data)
#             else:
#                 # 这种是数组类型
#                 data_name = f'{index}'
#                 dict_data[data_name] = row
#         else:
#             # 这种是非list类型
#             data_name = f'{index}'
#             dict_data[data_name] = row
#     return dict_data


# todo 测试取代上侧方法
def parse_data(df, temp):
    # 这个是最终插入ck的数据字典
    dict_data = copy.deepcopy(temp)
    for index, row in df.iloc[0].iteritems():
        # 去除以raw_data开头的字段
        if index.startswith("raw"):
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
        nest_depth = {}
        if isinstance(row, list):
            # 数组中是字典
            if isinstance(row[0], dict):
                # for key in row[0]:
                #     data_name = f'{index}.{key}'
                #     dict_data[data_name] = []
                count = 0
                for data in row:
                    count += 1
                    for key in data:
                        if count == 1:
                            filter_data = alter_data_type(data.get(key))
                            dict_data.get(f'{index}.{key}')[0] = filter_data
                        else:
                            filter_data = alter_data_type(data.get(key))
                            dict_data.get(f'{index}.{key}').append(filter_data)
                nest_depth[index] = count
            else:
                # 这种是数组类型
                data_name = f'{index}'
                dict_data[data_name] = row
        else:
            # 这种是非list类型
            data_name = f'{index}'
            dict_data[data_name] = row
    for data_key in dict_data:
        if data_key.find('.') and isinstance(dict_data.get(data_key), list):
            data = dict_data.get(data_key)
            data_len = len(data)
            nest_depth_len = nest_depth[data_key.split('.')[0]]
            if nest_depth_len > data_len:
                for i in range(nest_depth_len-data_len):
                    if isinstance(data[-1],str):
                        data.append("")
                    elif isinstance(data[-1],int):
                        data.append(0)
                    elif isinstance(data[-1],float):
                        data.append(0.0)

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
            logger.info("There is no data in the table")
    return fields_structure_dict


def utc_timestamp(date):
    format_date = time.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
    return int(time.mktime(format_date) * 1000)
