import copy
import datetime
import json
from json import JSONDecodeError

import pandas as pd
from opensearchpy import helpers

from oss_know.libs.base_dict.clickhouse import GITHUB_ISSUES_TIMELINE_TEMPLATE
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
from oss_know.libs.clickhouse.init_ck_transfer_data import parse_data_init, parse_data, get_table_structure
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


def opensearch_to_clickhouse(os_client, index_name, query_body, ck_client, table_template, table_name):
    os_result = helpers.scan(os_client, index=index_name, query=query_body, size=5000, scroll="20m",
                             request_timeout=120, preserve_order=True)

    bulk_data = []
    bulk_size = 10000  # TODO bulk_size should be defined outside by config
    num_inserted = 0
    ck_data_insert_at = now_timestamp()
    for os_data in os_result:
        row = timeline_doc_to_ck_row(os_data, ck_data_insert_at) \
            if index_name == OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE \
            else os_doc_to_ck_row(os_data, table_name, table_template, ck_client)
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


def os_doc_to_ck_row(os_doc, table_name, table_template, ck_client):
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

    fields = get_table_structure(table_name=table_name, ck=ck_client)
    for field in fields:
        if row.get(field) and fields.get(field) == 'DateTime64(3)':
            row[field] = datetime.datetime.strptime(row[field], '%Y-%m-%dT%H:%M:%SZ')
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
