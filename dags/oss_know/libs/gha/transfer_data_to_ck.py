# -*-coding:utf-8-*-
import datetime
import gzip
import json
import random
import time
import pandas as pd
from json import JSONDecodeError

from oss_know.libs.util.base import now_timestamp
from oss_know.libs.util.log import logger
from clickhouse_driver.errors import ServerException
from oss_know.libs.base_dict.variable_key import CK_TABLE_DEFAULT_VAL_TPLT
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data_init, get_table_structure, parse_data
from oss_know.libs.util.clickhouse_driver import CKServer


def un_gzip(gz_filename):
    try:
        filename = gz_filename.replace('.gz', '')
        g_file = gzip.GzipFile(gz_filename)
        open(f'{filename}', 'wb+').write(g_file.read())
        logger.info(f"{gz_filename}  Decompression completed")
        g_file.close()
    except Exception as e:
        logger.info(f"{gz_filename}Decompression failed ，failure reason：{e}")


def error_log(year, month, day, log_info, clickhouse_server_info):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    logger.info(year)
    logger.info(month)
    logger.info(day)
    logger.info(log_info)
    logger.info(datetime.datetime.now())
    logger.info(datetime.datetime.now().timestamp())

    current_datetime = datetime.datetime.now()
    current_timestamp = int(current_datetime.timestamp())
    error_data = [{"year": year, "month": month, "day": day, "error_info": log_info,
                   "update_date": datetime.datetime(current_datetime.year,
                                                    current_datetime.month,
                                                    current_datetime.day,
                                                    current_datetime.hour,
                                                    current_datetime.minute,
                                                    current_datetime.second), "update_timestamp": current_timestamp
                   }]
    logger.info(error_data)
    results = ck.execute(f"insert into table transfer_gha_error_log values", error_data)
    logger.info(results)
    ck.close()


def get_index_name(index_name):
    result = index_name[0].lower()
    for i in range(1, len(index_name)):
        if index_name[i].isupper():
            result = result + '_' + index_name[i].lower()
        else:
            result = result + index_name[i]
    return result


def parse_json_data(year, month, day, clickhouse_server_info):
    from airflow.models import Variable
    table_templates = Variable.get(CK_TABLE_DEFAULT_VAL_TPLT, deserialize_json=True)
    bulk_data_map = {}
    count_map = {}
    # for hour in [0]:
    for hour in range(24):
        file_name = f'{year}-{month}-{day}-{hour}.json'
        parse_json_data_hour(clickhouse_server_info=clickhouse_server_info,
                             file_name=file_name,
                             bulk_data_map=bulk_data_map,
                             count_map=count_map, table_templates=table_templates)
    # 插入数据收尾（不到20000）
    for event_type in bulk_data_map:
        bulk_data = bulk_data_map.get(event_type)
        if bulk_data:
            logger.info(f'start transfer {event_type} ................................')
            transfer_data_by_repo(clickhouse_server_info=clickhouse_server_info,
                                  table_name=event_type,
                                  tplt=table_templates.get(event_type), bulk_data=bulk_data)
            count_map[event_type] = count_map.get(event_type, 0) + len(bulk_data)
            logger.info(f"Successfully inserted {event_type} {count_map[event_type]}")
            bulk_data.clear()


def parse_json_data_hour(clickhouse_server_info, file_name, bulk_data_map, count_map,
                         table_templates):
    gh_archive_year = file_name.split('-')[0]
    gh_archive_month = file_name.split('-')[1]
    gh_archive_day = file_name.split('-')[2]
    gh_archive_hour = file_name.split('-')[3][0:-5]
    data_parents_path = f'/opt/airflow/gha/{gh_archive_year}/'
    logger.info(f"Start importing {file_name} data...................")
    try:
        with open(data_parents_path + file_name, 'r+') as f:
            lines = f.readlines()
            for line in lines:
                result = json.loads(line)
                # 如将 PushEvent 修改成 push_event
                event_type = get_index_name(result['type'])

                # 2015年之前和2015年之后的数据结构不一致
                if int(gh_archive_year) < 2015:
                    try:

                        owner = result['repository']['owner']
                        repo = result['repository']['name']


                    except KeyError as e:
                        owner = 'null'
                        repo = 'null'

                    event_type = event_type + "_old"
                else:
                    owner = result['repo']['name'].split('/')[0]
                    repo = result['repo']['name'].split('/')[1]
                raw_datas = bulk_data_map.get(event_type, [])
                raw_datas.append({"_index": event_type,
                                  "_source": {"search_key": {"owner": owner, "repo": repo,
                                                             "updated_at": int(
                                                                 datetime.datetime.now().timestamp() * 1000),
                                                             "gh_archive_year": gh_archive_year,
                                                             "gh_archive_month": gh_archive_month,
                                                             "gh_archive_day": gh_archive_day,
                                                             "gh_archive_hour": gh_archive_hour
                                                             },
                                              "raw_data": result
                                              }
                                  })
                if event_type != 'release_event_old':
                    bulk_data_map[event_type] = raw_datas

                for event_type in bulk_data_map:
                    bulk_data = bulk_data_map.get(event_type)
                    if len(bulk_data) == 40000:
                        logger.info(f'start transfer {event_type} ................................')
                        transfer_data_by_repo(clickhouse_server_info=clickhouse_server_info,
                                              table_name=event_type,
                                              tplt=table_templates.get(event_type), bulk_data=bulk_data)
                        count_map[event_type] = count_map.get(event_type, 0) + 40000
                        logger.info(f"Successfully inserted {event_type} {count_map[event_type]}")
                        bulk_data.clear()

            # client.close()
    except FileNotFoundError as e:

        error_log(log_info=str(e), year=int(gh_archive_year), month=int(gh_archive_month), day=int(gh_archive_day),
                  clickhouse_server_info=clickhouse_server_info)


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
    year = 0
    month = 0
    day = 0
    try:
        for os_data in opensearch_datas:
            # logger.info(os_data)
            # break
            updated_at = os_data["_source"]["search_key"]["updated_at"]
            if updated_at > max_timestamp:
                max_timestamp = updated_at
            df_data = os_data["_source"]
            year = df_data["search_key"]["gh_archive_year"]
            month = df_data["search_key"]["gh_archive_month"]
            day = df_data["search_key"]["gh_archive_day"]
            df = pd.json_normalize(df_data)
            template["ck_data_insert_at"] = now_timestamp()
            template["deleted"] = 0
            dict_data = parse_data(df, template)
            try:
                dict_dict = json.loads(json.dumps(dict_data))
            except JSONDecodeError as error:
                logger.error(error)
                continue
            for field in fields:
                if dict_dict.get(field) and fields.get(field) == 'DateTime64(3)':
                    try:
                        dict_dict[field] = datetime.datetime.strptime(dict_dict[field], '%Y-%m-%dT%H:%M:%SZ')
                    except ValueError as e:

                        dict_dict[field] = timestamp_to_utc(
                            datetime.datetime.strptime(dict_dict[field], '%Y-%m-%dT%H:%M:%S%z').timestamp())
                        dict_dict[field] = datetime.datetime.strptime(dict_dict[field], '%Y-%m-%dT%H:%M:%SZ')

                elif dict_dict.get(field) and fields.get(field) == 'Array(DateTime64(3))':
                    for i, v in enumerate(dict_dict.get(field)):
                        if v != '':
                            dict_dict[field][i] = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%SZ')
                elif fields.get(field) == 'String':
                    try:
                        dict_dict[field].encode('utf-8')
                    except UnicodeEncodeError as error:

                        dict_dict[field] = dict_dict[field].encode('unicode-escape').decode('utf-8')

            bulk_data.append(dict_dict)
            ck_sql = f"INSERT INTO {table_name} VALUES"



    # airflow dag的中断
    except Exception as error:
        error_log(log_info=str(error), year=int(year), month=int(month), day=int(day),
                  clickhouse_server_info=clickhouse_server_info)
        raise Exception(error)

    try:
        if bulk_data:
            ck.execute(ck_sql, bulk_data)
            time.sleep(random.randint(2, 3))

    except ServerException as error:
        logger.info(table_name)
        raise ServerException(error)
    except Exception as e:
        raise Exception
    ck.close()


def timestamp_to_utc(timestamp):
    # 10位时间戳
    return datetime.datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%dT%H:%M:%SZ")
