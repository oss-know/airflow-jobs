# -*-coding:utf-8-*-
import copy
import datetime
import gzip
import json
import os
import random
import time
from json import JSONDecodeError

import pandas as pd
from loguru import logger

import numpy
from opensearchpy import helpers, NotFoundError
from clickhouse_driver.errors import ServerException
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_RAW_DATA
from oss_know.libs.base_dict.variable_key import CK_TABLE_DEFAULT_VAL_TPLT
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.clickhouse_driver import CKServer


def un_gzip(gz_filename):
    try:
        filename = gz_filename.replace('.gz', '')
        # logger.info(filename)
        # open(f'{filename}', 'wb+')
        g_file = gzip.GzipFile(gz_filename)
        open(f'{filename}', 'wb+').write(g_file.read())
        logger.info(f"{gz_filename}  Decompression completed")
        g_file.close()
    except Exception as e:
        logger.info(f"{gz_filename}Decompression failed ，failure reason：{e}")


def get_index_name(index_name):
    result = index_name[0].lower()
    for i in range(1, len(index_name)):
        if index_name[i].isupper():
            result = result + '_' + index_name[i].lower()
        else:
            result = result + index_name[i]
    return result


def parse_json_data(year, month, day, clickhouse_server_info, opensearch_conn_datas):
    from airflow.models import Variable
    table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
    bulk_data_map = {}
    count_map = {}
    for hour in range(24):
        file_name = f'{year}-{month}-{day}-{hour}.json'
        un_gzip('/opt/airflow/gha/' + file_name + '.gz')
        parse_json_data_hour(clickhouse_server_info=clickhouse_server_info,
                             file_name=file_name,
                             opensearch_conn_infos=opensearch_conn_datas,
                             bulk_data_map=bulk_data_map,
                             count_map=count_map, table_templates=table_templates)
        try:
            os.remove(f'/opt/airflow/gha/{file_name}')
            logger.info(f"File deleted :{file_name}")
        except FileNotFoundError as e:
            logger.info(f"FileNotFound:{file_name}")
    for event_type in bulk_data_map:
        bulk_data = bulk_data_map.get(event_type)
        if bulk_data:
            transfer_data_by_repo(clickhouse_server_info=clickhouse_server_info,
                                  table_name=event_type,
                                  tplt=table_templates.get(event_type), bulk_data=bulk_data)
            count_map[event_type] = count_map.get(event_type, 0) + len(bulk_data)
            logger.info(f"Successfully inserted {event_type} {count_map[event_type]}")
            bulk_data.clear()


def parse_json_data_hour(clickhouse_server_info, file_name, opensearch_conn_infos, bulk_data_map, count_map,
                         table_templates):
    # client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_infos)

    gh_archive_year = file_name.split('-')[0]
    gh_archive_month = gh_archive_year + file_name.split('-')[1]
    gh_archive_day = gh_archive_month + file_name.split('-')[2]
    data_parents_path = '/opt/airflow/gha/'
    logger.info(f"Start importing {file_name} data...................")
    try:
        with open(data_parents_path + file_name, 'r+') as f:
            lines = f.readlines()
            bulk_datas_list = []

            for line in lines:
                result = json.loads(line)
                event_type = get_index_name(result['type'])
                # if event_type == 'issues_event':
                # logger.info("--------------------------------")
                if event_type == 'issues_event' or event_type == 'issue_comment_event':
                    # result['payload']['issue']['body'] = ''
                    # if event_type == 'issue_comment_event':
                    #     result['payload']['comment']['body'] = ''

                    owner = result['repo']['name'].split('/')[0]
                    repo = result['repo']['name'].split('/')[1]
                    if result['payload']:
                        number = result['payload']['issue']['number']
                    else:
                        logger.info(f"wrong data :{result}")
                        number = 0
                    ll = bulk_data_map.get(event_type, [])
                    ll.append({"_index": event_type,
                               "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
                                                          "updated_at": int(datetime.datetime.now().timestamp() * 1000),
                                                          "gh_archive_year": gh_archive_year,
                                                          "gh_archive_month": gh_archive_month,
                                                          "gh_archive_day": gh_archive_day},
                                           "raw_data": result}})
                    bulk_data_map[event_type] = ll

                elif event_type == 'pull_request_event' or event_type == 'pull_request_review_comment_event':
                    # result['payload']['pull_request']['body'] = ''
                    # if event_type == 'pull_request_review_comment_event':
                    #     result['payload']['comment']['body']=''
                    owner = result['repo']['name'].split('/')[0]
                    repo = result['repo']['name'].split('/')[1]
                    if result['payload']:
                        number = result['payload']['pull_request']['number']
                    else:
                        logger.info(f"wrong data :{result}")
                        number = 0
                    ll = bulk_data_map.get(event_type, [])
                    ll.append({"_index": event_type,
                               "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
                                                          "updated_at": int(datetime.datetime.now().timestamp() * 1000),
                                                          "gh_archive_year": gh_archive_year,
                                                          "gh_archive_month": gh_archive_month,
                                                          "gh_archive_day": gh_archive_day},
                                           "raw_data": result}})
                    bulk_data_map[event_type] = ll

                # if len(bulk_datas_list) % 1000 == 0:
                #     logger.info(bulk_datas_list)
                #     logger.info(len(bulk_datas_list))
                #     return
                for event_type in bulk_data_map:
                    bulk_data = bulk_data_map.get(event_type)
                    if len(bulk_data) == 10000:
                        transfer_data_by_repo(clickhouse_server_info=clickhouse_server_info,
                                              table_name=event_type,
                                              tplt=table_templates.get(event_type), bulk_data=bulk_data)
                        count_map[event_type] = count_map.get(event_type, 0) + 10000
                        logger.info(f"Successfully inserted {event_type} {count_map[event_type]}")
                        bulk_data.clear()

            # for i in bulk_datas_dict:
            #     helpers.bulk(client=client, actions=bulk_datas_dict.get(i))
            #     logger.info(f"already_insert_{len(bulk_datas_dict.get(i))}")
            #     logger.info(file_name)

            # client.close()
    except FileNotFoundError as e:
        logger.info(e)


# 从json直接导入
def transfer_data_by_repo(clickhouse_server_info, table_name, tplt,
                          bulk_data):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    df = pd.json_normalize(tplt)
    template = parse_data_init(df)
    opensearch_datas = bulk_data

    fields = get_table_structure(table_name=table_name, ck=ck)
    max_timestamp = 0
    count = 0
    bulk_data = []
    try:
        for os_data in opensearch_datas:
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
                    # except KeyError as e:
                    #     logger.info(dict_dict)
                    #     raise Exception(e)
            bulk_data.append(dict_dict)
            ck_sql = f"INSERT INTO {table_name} VALUES"

    # airflow dag的中断
    except Exception as error:
        raise Exception(error)
    # 处理尾部多余的数据
    try:
        if bulk_data:
            # random.shuffle(bulk_data)
            result = ck.execute(ck_sql, bulk_data)
            time.sleep(random.randint(2, 3))
        # logger.info(f'已经插入的数据的条数为:{count}')
    # except KeyError as error:
    #     bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
    #     raise KeyError(error)
    except ServerException as error:
        logger.info(table_name)
        # logger.info(dict_dict)
        raise ServerException(error)

    ck.close()


"""为opensearch"""


# def transfer_data_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, template,
#                           gh_archive_day):
#     ck = CKServer(host=clickhouse_server_info["HOST"],
#                   port=clickhouse_server_info["PORT"],
#                   user=clickhouse_server_info["USER"],
#                   password=clickhouse_server_info["PASSWD"],
#                   database=clickhouse_server_info["DATABASE"])
#
#     # if transfer_type == "github_git_init_by_repo":
#     #     search_key_owner = search_key['owner']
#     #     search_key_repo = search_key['repo']
#     #     # 判断项目是否在ck中存在
#     #     if_null_sql = f"select count() from {table_name} where search_key__owner='{search_key_owner}' and search_key__repo='{search_key_repo}'"
#     #     if_null_result = ck.execute_no_params(if_null_sql)
#     #     if if_null_result[0][0] != 0:
#     #         keep_idempotent(ck=ck, search_key=search_key, clickhouse_server_info=clickhouse_server_info,
#     #                         table_name=table_name, transfer_type="github_git_init_by_repo")
#     #         pass
#     #     else:
#     #         logger.info("No data in CK")
#     #     logger.info("github_git_init_by_repo------------------------")
#     #     opensearch_datas = get_data_from_opensearch_by_repo(index=opensearch_index,
#     #                                                         opensearch_conn_datas=opensearch_conn_datas,
#     #                                                         repo=search_key)
#     # elif transfer_type == "maillist_init":
#     #     search_key_project_name = search_key['project_name']
#     #     search_key_mail_list_name = search_key['mail_list_name']
#     #     if_null_sql = f"select count() from {table_name} where search_key__project_name='{search_key_project_name}' and search_key__mail_list_name='{search_key_mail_list_name}'"
#     #     if_null_result = ck.execute_no_params(if_null_sql)
#     #     if not if_null_result:
#     #         keep_idempotent(ck=ck, search_key=search_key, clickhouse_server_info=clickhouse_server_info,
#     #                         table_name=table_name, transfer_type="maillist_init")
#     #     logger.info("maillist------------------------")
#     #     opensearch_datas = get_data_from_opensearch_maillist(index=opensearch_index,
#     #                                                          opensearch_conn_datas=opensearch_conn_datas,
#     #                                                          maillist_repo=search_key)
#     opensearch_datas = get_data_from_opensearch_gha(index=opensearch_index,
#                                                     opensearch_conn_datas=opensearch_conn_datas,
#                                                     gh_archive_day=gh_archive_day)
#
#     fields = get_table_structure(table_name=table_name, ck=ck)
#     max_timestamp = 0
#     count = 0
#     bulk_data = []
#     try:
#         for os_data in opensearch_datas[0]:
#             updated_at = os_data["_source"]["search_key"]["updated_at"]
#             if updated_at > max_timestamp:
#                 max_timestamp = updated_at
#             df_data = os_data["_source"]
#             df = pd.json_normalize(df_data)
#             template["ck_data_insert_at"] = int(round(time.time() * 1000))
#             template["deleted"] = 0
#             dict_data = parse_data(df, template)
#             try:
#                 dict_dict = json.loads(json.dumps(dict_data))
#             except JSONDecodeError as error:
#                 logger.error(error)
#                 continue
#             for field in fields:
#                 if dict_dict.get(field) and fields.get(field) == 'DateTime64(3)':
#                     dict_dict[field] = datetime.datetime.strptime(dict_dict[field], '%Y-%m-%dT%H:%M:%SZ')
#                 elif fields.get(field) == 'String':
#                     try:
#                         dict_dict[field].encode('utf-8')
#                     except UnicodeEncodeError as error:
#                         dict_dict[field] = dict_dict[field].encode('unicode-escape').decode('utf-8')
#             bulk_data.append(dict_dict)
#             ck_sql = f"INSERT INTO {table_name} VALUES"
#             try:
#                 # result = ck.execute(ck_sql, [dict_data])
#                 count += 1
#                 if count % 20000 == 0:
#                     # random.shuffle(bulk_data)
#                     result = ck.execute(ck_sql, bulk_data)
#                     bulk_data.clear()
#                     max_timestamp = 0
#                     logger.info(f'已经插入的数据的条数为:{count}')
#             except KeyError as error:
#                 raise KeyError(error)
#             except ServerException as error:
#                 raise ServerException(error)
#             except AttributeError as error:
#                 raise AttributeError(error)
#     # airflow dag的中断
#     except AirflowException as error:
#         raise AirflowException(f'airflow interrupt {error}')
#     except NotFoundError as error:
#         raise NotFoundError(
#             f'scroll error raise HTTP_EXCEPTIONS.get(status_code, TransportError)(opensearchpy.exceptions.NotFoundError: NotFoundError(404, "search_phase_execution_exception", "No search context found for id [631]"){error}')
#     except Exception as error:
#         raise Exception(error)
#     # 处理尾部多余的数据
#     # try:
#     if bulk_data:
#         # random.shuffle(bulk_data)
#         result = ck.execute(ck_sql, bulk_data)
#     logger.info(f'已经插入的数据的条数为:{count}')
#     # except KeyError as error:
#     #     bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
#     #     raise KeyError(error)
#     # except ServerException as error:
#     #     bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
#     #     raise ServerException(error)
#     # except AttributeError as error:
#     #     bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
#     #     raise AttributeError(error)
#     # except Exception as error:
#     #     bulk_except_repo(bulk_data, opensearch_datas, opensearch_index, table_name, search_key, transfer_type)
#     #     raise Exception(error)
#     # 将检查点放在这里插入
#
#
#         # time.sleep(10)
#         # if not if_data_eq_maillist(count=count, ck=ck, table_name=table_name,
#         #                            project_name=search_key.get('project_name'),
#         #                            mail_list_name=search_key.get('mail_list_name')):
#         #     raise Exception("Inconsistent data between opensearch and clickhouse")
#         # else:
#         #     logger.info("opensearch and clickhouse data are consistent")
#
#     ck.close()


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
    # logger.info(fields_structure_dict)
    return fields_structure_dict


def get_data_from_opensearch_gha(index, opensearch_conn_datas, gh_archive_day):
    opensearch_client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_datas)
    results = helpers.scan(client=opensearch_client,
                           query={
                               "query": {
                                   "bool": {
                                       "must": [
                                           {
                                               "term": {
                                                   "search_key.gh_archive_day.keyword": {
                                                       "value": gh_archive_day
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
