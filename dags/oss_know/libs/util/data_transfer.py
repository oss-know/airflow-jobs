import copy
import datetime
import json
import re
import time
from json import JSONDecodeError

import numpy
import pandas as pd
from opensearchpy import helpers

from oss_know.libs.base_dict.clickhouse import GITHUB_ISSUES_TIMELINE_TEMPLATE, CLICKHOUSE_RAW_DATA
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.util.base import get_opensearch_client, now_timestamp
from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.log import logger


def sync_clickhouse_repos_from_opensearch(owner_repos, index_name, opensearch_conn_info,
                                          table_name, clickhouse_conn_info, row_template):
    logger.info(f'Syncing {len(owner_repos)} repos from opensearch to clickhouse')
    for owner_repo in owner_repos:
        owner = owner_repo['owner']
        repo = owner_repo['repo']
        sync_clickhouse_from_opensearch(owner, repo, index_name, opensearch_conn_info,
                                        table_name, clickhouse_conn_info, row_template)


# Copy the data from opensearch to clickhouse, whose 'updated_at' is greater than the max 'updated_at'
# in ClickHouse
def sync_clickhouse_from_opensearch(owner, repo, index_name, opensearch_conn_info,
                                    table_name, clickhouse_conn_info, row_template):
    ck_client = CKServer(
        host=clickhouse_conn_info.get('HOST'),
        port=clickhouse_conn_info.get('PORT'),
        user=clickhouse_conn_info.get('USER'),
        password=clickhouse_conn_info.get('PASSWD'),
        database=clickhouse_conn_info.get('DATABASE'),
    )
    os_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_info)

    os_search_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "search_key.owner": owner
                        }
                    },
                    {
                        "match": {
                            "search_key.repo": repo
                        }
                    }
                ]
            }
        }
    }

    result = ck_client.execute_no_params(f'''
    SELECT search_key__updated_at
    FROM {table_name}
    WHERE search_key__owner = '{owner}'
      AND search_key__repo = '{repo}'
    ORDER BY search_key__updated_at DESC
    LIMIT 1
    ''')
    if result:
        # result is a list with one tuple
        range_condition = {
            'range': {
                'search_key.updated_at': {
                    'gt': int(result[0][0])
                }
            }
        }
        os_search_query['query']['bool']['must'].append(range_condition)
    logger.info(f'Query opensearch {index_name} with {os_search_query}')
    opensearch_to_clickhouse(os_client, index_name, os_search_query,
                             ck_client, table_template=row_template, table_name=table_name)


# 用于拆分discourse topic content中的post内容
def discourse_topic_content_split_post(table_name, index_name, os_result):
    if table_name == 'discourse_topic_content_posts':
        # To alias with below code.
        opensearch_datas_part = []
        for os_data in os_result:
            # 注意copy.deepcopy才是深拷贝
            for post in os_data['_source']['raw_data']['post_stream']['posts']:
                cur = copy.deepcopy(os_data)
                cur['_source']['raw_data'] = copy.deepcopy(post)
                opensearch_datas_part.append(cur)
        opensearch_datas = opensearch_datas_part
    elif table_name == 'discourse_topic_content_info':
        opensearch_datas_part = []
        for os_data in os_result:
            cur = copy.deepcopy(os_data)
            del cur['_source']['raw_data']['post_stream']['posts']
            # 将suggested_topics的字典内容压平
            if 'suggested_topics' in cur['_source']['raw_data']:
                cur['_source']['raw_data']['suggested_topics'] = json.dumps(
                    cur['_source']['raw_data']['suggested_topics'])
            else:
                cur['_source']['raw_data']['suggested_topics'] = None
            opensearch_datas_part.append(cur)
        opensearch_datas = opensearch_datas_part
    return opensearch_datas


def get_data_from_opensearch(index, opensearch_conn_datas):
    opensearch_client = get_opensearch_client(opensearch_conn_info=opensearch_conn_datas)
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
                           request_timeout=120,
                           preserve_order=True)
    return results, opensearch_client


def get_opensearch_query_body(query_type, owner_or_project_name, repo_or_mail_list_name):
    if query_type == 'owner_repo':
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "search_key.owner.keyword": {
                                    "value": owner_or_project_name
                                }
                            }
                        },
                        {
                            "term": {
                                "search_key.repo.keyword": {
                                    "value": repo_or_mail_list_name
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
    else:
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "search_key.project_name.keyword": {
                                    "value": owner_or_project_name
                                }
                            }
                        },
                        {
                            "term": {
                                "search_key.mail_list_name.keyword": {
                                    "value": repo_or_mail_list_name
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
    return query_body


def opensearch_to_clickhouse(os_client, index_name, query_body, ck_client, table_template, table_name):
    os_result = helpers.scan(os_client, index=index_name, query=query_body, size=5000, scroll="20m",
                             request_timeout=120, preserve_order=True)
    if index_name == 'discourse_topic_content':
        os_result = discourse_topic_content_split_post(table_name, index_name, os_result)

    bulk_data = []

    bulk_size = 20000  # TODO bulk_size should be defined outside by config

    num_inserted = 0
    fields = get_table_structure(table_name=table_name, ck=ck_client)
    ck_data_insert_at = now_timestamp()
    for os_data in os_result:
        row = timeline_doc_to_ck_row(os_data, ck_data_insert_at) \
            if index_name == OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE \
            else os_doc_to_ck_row(os_data, table_template, fields)
        bulk_data.append(row)

        if len(bulk_data) >= bulk_size:
            ck_client.execute(f'insert into {table_name} (*) values', bulk_data)
            num_inserted += len(bulk_data)
            logger.info(f'{num_inserted} rows inserted into {table_name}')
            bulk_data = []
            ck_data_insert_at = now_timestamp()

    if bulk_data:
        ck_client.execute(f'insert into {table_name} (*) values', bulk_data)
        num_inserted += len(bulk_data)
    logger.info(f'{num_inserted} rows inserted into {table_name}')


def os_doc_to_ck_row(os_doc, table_template, fields):
    normalized_template = pd.json_normalize(table_template)
    template = parse_data_init(normalized_template)
    template["ck_data_insert_at"] = now_timestamp()

    source_data = os_doc["_source"]
    normalized_data = pd.json_normalize(source_data)

    dict_data = parse_data(normalized_data, template)

    try:
        row = json.loads(json.dumps(dict_data))
    except JSONDecodeError as error:
        logger.error(error)
        return {}

    for field in fields:
        if row.get(field) and fields.get(field) == 'DateTime64(3)':
            row[field] = try_parsing_date(row.get(field))
        elif row.get(field) and fields.get(field) == 'Array(DateTime64(3))':
            for i, k in enumerate(row.get(field)):
                if k != 'null':
                    row[field][i] = try_parsing_date(k)
                else:
                    row[field][i] = try_parsing_date("1999-03-01T16:47:08.370Z")
        elif fields.get(field) == 'String':
            try:
                row[field].encode('utf-8')
            except UnicodeEncodeError:
                row[field] = row[field].encode('unicode-escape').decode('utf-8')

    return row


def timeline_doc_to_ck_row(timeline_doc, ck_data_insert_at):
    row = copy.deepcopy(GITHUB_ISSUES_TIMELINE_TEMPLATE)
    row['search_key__owner'] = timeline_doc["_source"]["search_key"]['owner']
    row['search_key__repo'] = timeline_doc["_source"]["search_key"]['repo']
    row['search_key__number'] = timeline_doc["_source"]["search_key"]['number']
    row['search_key__event'] = timeline_doc["_source"]["search_key"]['event']
    row['search_key__updated_at'] = timeline_doc["_source"]["search_key"]['updated_at']
    row['ck_data_insert_at'] = ck_data_insert_at
    raw_data = timeline_doc["_source"]["raw_data"]
    standard_data = json.dumps(raw_data, separators=(',', ':'), ensure_ascii=False)
    row['timeline_raw'] = standard_data
    return row


def try_parsing_date(text):
    for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f+00:00'):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')


def np_type_2_py_type(row):
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


regex = r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'

match_iso8601 = re.compile(regex).match


# 判断是不是iso8601 格式字符串
def validate_iso8601(str_val):
    try:
        if match_iso8601(str_val) is not None:
            return True
    except:
        pass
    return False


# 判断字符串是不是utc日期格式
def datetime_valid(dt_str):
    try:
        datetime.datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')
    except:
        return False
    return True


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
        row = np_type_2_py_type(row)
        # # 这里的字符串都转换成在ck中能够使用函数解析json的标准格式
        # if isinstance(row, str):
        #     row.replace(": ", ":")
        # 解决嵌套 array
        if isinstance(row, list):
            # 数组中是字典
            if row and isinstance(row[0], dict):
                default_dict = {}
                for key in dict_data:
                    if key.startswith(f'{index}.'):
                        # default_dict 保存模板中的默认值，0, None, {} 等
                        default_dict[key] = dict_data[key][0]
                        dict_data[key] = []
                for data in row:
                    vis = {}
                    for key in data:
                        # 若不在模板中，之后授予default value
                        if (f'{index}.{key}' not in dict_data):
                            continue
                        vis[f'{index}.{key}'] = 1
                        filter_data = np_type_2_py_type(data.get(key), default_dict.get(f'{index}.{key}'))
                        try:
                            # BUG 如果一个list包含3个dict，其中只有第二个有某个键值对
                            #     那这就会有问题。
                            #     目前解决办法：通过下面的default_dict处理
                            dict_data.get(f'{index}.{key}').append(filter_data)
                        except Exception as e:
                            logger.info(f'{index}.{key}')
                            raise e
                    # chenkx :: 若不存在该key，则添加默认值:"0,'',None,{}等"，保证list的完整性
                    for key in default_dict:
                        if key not in vis:
                            dict_data[key].append(default_dict[key])
            else:
                # 这种是数组类型
                data_name = f'{index}'
                if data_name in dict_data:
                    dict_data[data_name] = row
        else:
            # 这种是非list类型
            data_name = f'{index}'
            if data_name in dict_data:
                dict_data[data_name] = row

    return dict_data


def parse_data_init(df):
    dict_data = {}
    for index, row in df.iloc[0].iteritems():
        # 去除以raw_data开头的字段
        if index.startswith(CLICKHOUSE_RAW_DATA):
            index = index[9:]
        index = index.replace('.', '__')

        row = np_type_2_py_type(row)

        # 解决嵌套array
        if isinstance(row, list):
            # 数组中是字典
            if isinstance(row[0], dict):
                for key in row[0]:
                    data_name = f'{index}.{key}'
                    dict_data[data_name] = []
                for data in row:
                    for key in data:
                        filter_data = np_type_2_py_type(data.get(key))
                        dict_data.get(f'{index}.{key}').append(filter_data)
            else:
                # 数组类型
                data_name = f'{index}'
                dict_data[data_name] = row
        else:
            # 非list类型
            data_name = f'{index}'
            dict_data[data_name] = row
    return dict_data


# 判断opensearch和ck数据是否一致

def if_data_eq_github(count, ck, table_name, owner, repo):
    sql = f"select count() from {table_name} where search_key__owner='{owner}' and search_key__repo='{repo}'"
    # logger.info(sql)
    result = ck.execute_no_params(sql)
    logger.info(f'data count in ck {result[0][0]}')
    return count == result[0][0]


def if_data_eq_maillist(count, ck, table_name, project_name, mail_list_name):
    sql = f"select count() from {table_name} where search_key__project_name='{project_name}'" \
          f"and search_key__mail_list_name='{mail_list_name}'"
    # logger.info(sql)
    result = ck.execute_no_params(sql)
    # logger.info(result)
    logger.info(f'data count in ck {result[0][0]}')
    return count == result[0][0]


# 保证插入数据的幂等性
def keep_idempotent(ck, search_key, clickhouse_server_info, table_name, transfer_type):
    # 获取clickhouse集群节点信息
    get_clusters_sql = f"select host_name from system.clusters" \
                       f" where cluster = '{clickhouse_server_info['CLUSTER_NAME']}'"

    clusters_info = ck.execute_no_params(get_clusters_sql)
    delect_data_sql = ''
    check_if_done_sql = ''
    # 保证幂等

    # 获得当前时间时间戳
    now = int(time.time())
    if transfer_type == 'github_git_init_by_repo' or transfer_type == 'github_issues_timeline_by_repo':
        owner = search_key.get('owner')
        repo = search_key.get('repo')
        delect_data_sql = f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']} " \
                          f"DELETE WHERE {now}={now} and search_key__owner = '{owner}' and search_key__repo = '{repo}'"
        check_if_done_sql = f"select is_done from system.mutations m WHERE `table` = '{table_name}_local'" \
                            f"and command = 'DELETE WHERE ({now} = {now}) AND (search_key__owner = \\'{owner}\\')" \
                            f"AND (search_key__repo = \\'{repo}\\')'"
    elif transfer_type == 'maillist_init':
        project_name = search_key.get('project_name')
        mail_list_name = search_key.get('mail_list_name')
        delect_data_sql = f"ALTER TABLE {table_name}_local ON CLUSTER {clickhouse_server_info['CLUSTER_NAME']}" \
                          f"DELETE WHERE {now} = {now} and search_key__project_name = '{project_name}'" \
                          f"and search_key__mail_list_name = '{mail_list_name}'"
        check_if_done_sql = f"select is_done from system.mutations m WHERE `table` = '{table_name}_local' " \
                            f"AND command = 'DELETE WHERE ({now} = {now}) " \
                            f"AND (search_key__project_name = \\'{project_name}\\') " \
                            f"AND (search_key__mail_list_name = \\'{mail_list_name}\\')'"
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
