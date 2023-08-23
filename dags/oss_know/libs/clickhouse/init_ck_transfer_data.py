import copy
import datetime
import json
import warnings
from json import JSONDecodeError

import pandas as pd
from airflow.exceptions import AirflowException

from oss_know.libs.base_dict.clickhouse import GITHUB_ISSUES_TIMELINE_TEMPLATE
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_CHECK_SYNC_DATA
from oss_know.libs.util.base import get_opensearch_client, now_timestamp
from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.data_transfer import parse_data, opensearch_to_clickhouse, keep_idempotent, \
    get_table_structure, \
    get_data_from_opensearch, get_opensearch_query_body
from oss_know.libs.util.log import logger


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


def ck_check_point_discourse(opensearch_client, opensearch_index, clickhouse_table, updated_at, repo):
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


# 特殊情况 by repo
def transfer_issue_timeline_by_repo(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas,
                                    search_key):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    github_issues_timeline_template = copy.deepcopy(GITHUB_ISSUES_TIMELINE_TEMPLATE)
    github_issues_timeline_template["ck_data_insert_at"] = now_timestamp()
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                         port=clickhouse_server_info["PORT"],
                         user=clickhouse_server_info["USER"],
                         password=clickhouse_server_info["PASSWD"],
                         database=clickhouse_server_info["DATABASE"])

    owner = search_key["owner"]
    repo = search_key["repo"]
    # 判断项目是否在ck中存在
    if_null_sql = f"select count() from {table_name} where search_key__owner='{owner}' and search_key__repo='{repo}'"
    if_null_result = ck_client.execute_no_params(if_null_sql)
    # TODO Test the mergexxxxx new Engine, keep_idempotent is not necessary.
    # if if_null_result[0][0] != 0:
    #     keep_idempotent(ck=ck_client, search_key=search_key, clickhouse_server_info=clickhouse_server_info,
    #                     table_name=table_name, transfer_type="github_git_init_by_repo")
    # else:
    #     logger.info("No data in CK")
    query_body = {
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

        },
        "sort": [
            {
                "search_key.updated_at": {
                    "order": "asc"
                }
            }
        ]
    }
    template = {}
    opensearch_to_clickhouse(os_client=opensearch_client,
                             index_name=opensearch_index,
                             query_body=query_body,
                             ck_client=ck_client,
                             table_template=template,
                             table_name=table_name)
    ck_client.close()


# 特殊情况 timeline 结构过于复杂 所以没有解析原始数据结构，将原始数据以字符串形式进行存储
def transfer_data_special(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    template = {
    }
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                         port=clickhouse_server_info["PORT"],
                         user=clickhouse_server_info["USER"],
                         password=clickhouse_server_info["PASSWD"],
                         database=clickhouse_server_info["DATABASE"])
    query_body = {
        "query": {"match_all": {}},
        "sort": [
            {
                "search_key.updated_at": {
                    "order": "asc"
                }
            }
        ]
    }
    opensearch_to_clickhouse(os_client=opensearch_client,
                             index_name=opensearch_index,
                             query_body=query_body,
                             ck_client=ck_client,
                             table_template=template,
                             table_name=table_name)

    ck_client.close()


warnings.filterwarnings('ignore')


def transfer_data(clickhouse_server_info, opensearch_index, table_name, opensearch_conn_datas, template):
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                         port=clickhouse_server_info["PORT"],
                         user=clickhouse_server_info["USER"],
                         password=clickhouse_server_info["PASSWD"],
                         database=clickhouse_server_info["DATABASE"])
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
    query_body = {
        "query": {"match_all": {}},
        "sort": [
            {
                "search_key.updated_at": {
                    "order": "asc"
                }
            }
        ]
    }
    opensearch_to_clickhouse(os_client=opensearch_client,
                             index_name=opensearch_index,
                             query_body=query_body,
                             ck_client=ck_client,
                             table_template=template,
                             table_name=table_name)
    ck_client.close()


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


def transfer_data_by_repo(clickhouse_server_info, table_name, opensearch_index, opensearch_conn_datas, template,
                          owner_repo_or_project_maillist_name, transfer_type):
    ck_client = CKServer(host=clickhouse_server_info["HOST"],
                         port=clickhouse_server_info["PORT"],
                         user=clickhouse_server_info["USER"],
                         password=clickhouse_server_info["PASSWD"],
                         database=clickhouse_server_info["DATABASE"])
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)

    if transfer_type == "github_git_init_by_repo":
        search_key_owner = owner_repo_or_project_maillist_name['owner']
        search_key_repo = owner_repo_or_project_maillist_name['repo']
        # 判断项目是否在ck中存在
        if_null_sql = f"select count() from {table_name} where search_key__owner='{search_key_owner}' and " \
                      f"search_key__repo='{search_key_repo}'"
        if_null_result = ck_client.execute_no_params(if_null_sql)
        if if_null_result[0][0] != 0:
            # keep_idempotent(ck=ck_client, search_key=owner_repo_or_project_maillist_name,
            #                 clickhouse_server_info=clickhouse_server_info,
            #                 table_name=table_name, transfer_type="github_git_init_by_repo")
            logger.info("The project data already exists")
        else:
            logger.info("No data in CK")
        logger.info("github_git_init_by_repo------------------------")
        query_body = get_opensearch_query_body("owner_repo", search_key_owner, search_key_repo)

    elif transfer_type == "maillist_init":
        search_key_project_name = owner_repo_or_project_maillist_name['project_name']
        search_key_mail_list_name = owner_repo_or_project_maillist_name['mail_list_name']
        if_null_sql = f"select count() from {table_name} where search_key__project_name='{search_key_project_name}' " \
                      f"and search_key__mail_list_name='{search_key_mail_list_name}'"
        if_null_result = ck_client.execute_no_params(if_null_sql)
        if not if_null_result:
            keep_idempotent(ck=ck_client,
                            search_key=owner_repo_or_project_maillist_name,
                            clickhouse_server_info=clickhouse_server_info,
                            table_name=table_name,
                            transfer_type="maillist_init")
        logger.info("maillist------------------------")
        query_body = get_opensearch_query_body("mail_list", search_key_project_name, search_key_mail_list_name)

    elif transfer_type == "discourse":

        search_key_owner = owner_repo_or_project_maillist_name['owner']
        search_key_repo = owner_repo_or_project_maillist_name['repo']
        logger.info("discourse------------------------")
        query_body = get_opensearch_query_body("owner_repo", search_key_owner, search_key_repo)

    opensearch_to_clickhouse(os_client=opensearch_client,
                             index_name=opensearch_index,
                             query_body=query_body,
                             ck_client=ck_client,
                             table_template=template,
                             table_name=table_name)

    ck_client.close()


#  TODO 测试取代老版本json转clickhouse法

# def parse_data(df, temp):
#     # 这个是最终插入ck的数据字典
#     dict_data = copy.deepcopy(temp)
#     for index, row in df.iloc[0].items():
#         # 去除以raw_data开头的字段
#         if index.startswith("raw"):
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
#         nest_depth = {}
#         if isinstance(row, list):
#             # 数组中是字典
#             if isinstance(row[0], dict):
#                 # for key in row[0]:
#                 #     data_name = f'{index}.{key}'
#                 #     dict_data[data_name] = []
#                 count = 0
#                 for data in row:
#                     count += 1
#                     for key in data:
#                         if count == 1:
#                             filter_data = alter_data_type(data.get(key))
#                             dict_data.get(f'{index}.{key}')[0] = filter_data
#                         else:
#                             filter_data = alter_data_type(data.get(key))
#                             dict_data.get(f'{index}.{key}').append(filter_data)
#                 nest_depth[index] = count
#             else:
#                 # 这种是数组类型
#                 data_name = f'{index}'
#                 dict_data[data_name] = row
#         else:
#             # 这种是非list类型
#             data_name = f'{index}'
#             dict_data[data_name] = row
#     for data_key in dict_data:
#         if data_key.find('.') and isinstance(dict_data.get(data_key), list):
#             data = dict_data.get(data_key)
#             data_len = len(data)
#             nest_depth_len = nest_depth[data_key.split('.')[0]]
#             if nest_depth_len > data_len:
#                 for i in range(nest_depth_len-data_len):
#                     if isinstance(data[-1],str):
#                         data.append("")
#                     elif isinstance(data[-1],int):
#                         data.append(0)
#                     elif isinstance(data[-1],float):
#                         data.append(0.0)


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
    logger.error(
        f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')


def bulk_except_maillist(bulk_data, opensearch_datas, opensearch_index, table_name, maillist_repo):
    start_updated_at = bulk_data[0]['search_key__updated_at']
    end_updated_at = bulk_data[-1]['search_key__updated_at']
    ck_check_point_maillist(opensearch_client=opensearch_datas[1],
                            opensearch_index=opensearch_index,
                            clickhouse_table=table_name,
                            updated_at=start_updated_at, maillist_repo=maillist_repo)
    logger.error(
        f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')


def bulk_except(bulk_data, opensearch_datas, opensearch_index, table_name):
    start_updated_at = bulk_data[0]['search_key__updated_at']
    end_updated_at = bulk_data[-1]['search_key__updated_at']
    ck_check_point(opensearch_client=opensearch_datas[1],
                   opensearch_index=opensearch_index,
                   clickhouse_table=table_name,
                   updated_at=start_updated_at)
    logger.error(
        f'批量插入出现问题，将这一批的最小的updated时间插入check_point 这一批数据的updated 范围为{start_updated_at}----{end_updated_at}')
