# -*-coding:utf-8-*-
import datetime
import gzip
import json
import random
import time
from opensearchpy import OpenSearch, helpers

from oss_know.libs.util.base import get_opensearch_client


# client = OpenSearch(
#     hosts=[{'host': "192.168.8.110", 'port': 19201}],
#     http_compress=True,
#     http_auth=("admin", "admin"),
#     use_ssl=True,
#     verify_certs=False,
#     ssl_assert_hostname=False,
#     ssl_show_warn=False
# )


def un_gzip(gz_filename):
    filename = gz_filename.replace('.gz', '')
    print(filename)
    g_file = gzip.GzipFile(gz_filename)
    open(f'{filename}', 'wb+').write(g_file.read())
    g_file.close()


"""更改为只有event类型为 pull_request_event issues_event issue_comment_event pull_request_review_comment_event"""


def parse_json_data(file_name, opensearch_conn_infos):
    client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_infos)
    gh_archive_year = file_name.split('-')[0]
    gh_archive_month = gh_archive_year + file_name.split('-')[1]
    gh_archive_day = gh_archive_month + file_name.split('-')[2]
    data_parents_path = '/opt/airflow/gha/'
    print(file_name)
    try:
        with open(data_parents_path + file_name, 'r+') as f:
            lines = f.readlines()
            bulk_datas_list = []
            count = 0
            for line in lines:
                result = json.loads(line)
                event_type = get_index_name(result['type'])
                # if event_type == 'issues_event':
                # print("--------------------------------")
                if event_type == 'issues_event' or event_type == 'issue_comment_event':
                    result['payload']['issue']['body'] = ''
                    if event_type == 'issue_comment_event':
                        result['payload']['comment']['body'] = ''
                    owner = result['repo']['name'].split('/')[0]
                    repo = result['repo']['name'].split('/')[1]
                    if result['payload']:
                        number = result['payload']['issue']['number']
                    else:
                        print(f"wrong data :{result}")
                        number = 0

                    bulk_datas_list.append(
                        {"_index": event_type, "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
                                                                          "updated_at":int(datetime.datetime.now().timestamp() * 1000),
                                                                          "gh_archive_year": gh_archive_year,
                                                                          "gh_archive_month": gh_archive_month,
                                                                          "gh_archive_day": gh_archive_day},
                                                           "raw_data": result}})
                elif event_type == 'pull_request_event' or event_type == 'pull_request_review_comment_event':
                    result['payload']['pull_request']['body'] = ''
                    if event_type == 'pull_request_review_comment_event':
                        result['payload']['comment']['body'] = ''
                    owner = result['repo']['name'].split('/')[0]
                    repo = result['repo']['name'].split('/')[1]
                    if result['payload']:
                        number = result['payload']['pull_request']['number']
                    else:
                        print(f"wrong data :{result}")
                        number = 0
                    bulk_datas_list.append(
                        {"_index": event_type, "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
                                                                          "updated_at": int(datetime.datetime.now().timestamp() * 1000),
                                                                          "gh_archive_year": gh_archive_year,
                                                                          "gh_archive_month": gh_archive_month,
                                                                          "gh_archive_day": gh_archive_day
                                                                          },
                                                           "raw_data": result}})
                # if len(bulk_datas_list) % 1000 == 0:
                #     print(bulk_datas_list)
                #     print(len(bulk_datas_list))
                #     return
                if len(bulk_datas_list) == 500:
                    # chunk_size
                    helpers.bulk(client=client, actions=bulk_datas_list)
                    # helpers.parallel_bulk(client=client, actions=bulk_datas_list, thread_count=2)
                    print("---")
                    count += 500
                    print(f"already insert {count}")
                    bulk_datas_list.clear()
                    time.sleep(3)

            if bulk_datas_list:
                helpers.bulk(client=client, actions=bulk_datas_list)
                # helpers.parallel_bulk(client=client, actions=bulk_datas_list, thread_count=2,chunk_size=2000)
                print("flag--------------------------------")
                count += len(bulk_datas_list)
                bulk_datas_list.clear()
                print(f"already insert {count}")
            time.sleep(30)

            # for i in bulk_datas_dict:
            #     helpers.bulk(client=client, actions=bulk_datas_dict.get(i))
            #     print(f"already_insert_{len(bulk_datas_dict.get(i))}")
            #     print(file_name)

            client.close()
    except FileNotFoundError as e:
        print(e)


"""所有种类的event"""


# def parse_json_data(file_name, opensearch_conn_infos):
#     client = get_opensearch_client(opensearch_conn_infos=opensearch_conn_infos)
#     gh_archive_year = file_name.split('-')[0]
#     gh_archive_month = gh_archive_year + file_name.split('-')[1]
#     gh_archive_day = gh_archive_month + file_name.split('-')[2]
#     data_parents_path = '/opt/airflow/gha/'
#     print(file_name)
#     with open(data_parents_path+file_name, 'r+') as f:
#         lines = f.readlines()
#         bulk_datas_dict = {}
#         bulk_datas_list = []
#         count = 0
#         for line in lines:
#             result = json.loads(line)
#             event_type = get_index_name(result['type'])
#             # ll = bulk_datas_dict.get(event_type, [])
#             if event_type == 'commit_comment_event' or \
#                     event_type == 'create_event' or \
#                     event_type == 'delete_event' or \
#                     event_type == 'fork_event' or \
#                     event_type == 'gollum_event' or \
#                     event_type == 'member_event' or \
#                     event_type == 'push_event' or \
#                     event_type == 'release_event' or \
#                     event_type == 'public_event' or \
#                     event_type == 'watch_event':
#                 owner = result['repo']['name'].split('/')[0]
#                 repo = result['repo']['name'].split('/')[1]
#                 bulk_datas_list.append({"_index": event_type, "_source": {"search_key": {"owner": owner,
#                                                                             "repo": repo,
#                                                                             "gh_archive_year": gh_archive_year,
#                                                                             "gh_archive_month": gh_archive_month,
#                                                                             "gh_archive_day": gh_archive_day},
#                                                              "raw_data": result}})
#             elif event_type == 'issues_event' or event_type == 'issue_comment_event':
#                 owner = result['repo']['name'].split('/')[0]
#                 repo = result['repo']['name'].split('/')[1]
#                 if result['payload']:
#                     number = result['payload']['issue']['number']
#                 else:
#                     print(f"wrong data :{result}")
#                     number = 0
#
#                 bulk_datas_list.append(
#                     {"_index": event_type, "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
#                                                                       "gh_archive_year": gh_archive_year,
#                                                                       "gh_archive_month": gh_archive_month,
#                                                                       "gh_archive_day": gh_archive_day},
#                                                        "raw_data": result}})
#             elif event_type == 'pull_request_event':
#                 owner = result['repo']['name'].split('/')[0]
#                 repo = result['repo']['name'].split('/')[1]
#                 if result['payload']:
#                     number = result['payload']['pull_request']['number']
#                 else:
#                     print(f"wrong data :{result}")
#                     number = 0
#                 bulk_datas_list.append(
#                     {"_index": event_type, "_source": {"search_key": {"owner": owner, "repo": repo, "number": number,
#                                                                       "gh_archive_year": gh_archive_year,
#                                                                       "gh_archive_month": gh_archive_month,
#                                                                       "gh_archive_day": gh_archive_day
#                                                                       },
#                                                        "raw_data": result}})
#             else:
#                 bulk_datas_list.append({"_index": event_type, "_source": result})
#             if len(bulk_datas_list) % 1000 == 0:
#                 # chunk_size
#                 helpers.bulk(client=client, actions=bulk_datas_list)
#                 # helpers.parallel_bulk(client=client, actions=bulk_datas_list, thread_count=2)
#                 print("---")
#                 count += 1000
#                 print(f"already insert {count}")
#                 time.sleep(0.5)
#                 bulk_datas_list.clear()
#         if bulk_datas_list:
#             helpers.bulk(client=client, actions=bulk_datas_list)
#             # helpers.parallel_bulk(client=client, actions=bulk_datas_list, thread_count=2,chunk_size=2000)
#             print("flag--------------------------------")
#             count += len(bulk_datas_list)
#             bulk_datas_list.clear()
#             print(f"already insert {count}")
#         time.sleep(3)
#
#         # for i in bulk_datas_dict:
#         #     helpers.bulk(client=client, actions=bulk_datas_dict.get(i))
#         #     print(f"already_insert_{len(bulk_datas_dict.get(i))}")
#         #     print(file_name)
#
#         client.close()

# un_gzip('2019-01-01-0.json.gz')
def create_index():
    index_list = ['CommitCommentEvent',
                  'CreateEvent',
                  'DeleteEvent',
                  'ForkEvent',
                  'GollumEvent',
                  'IssueCommentEvent',
                  'IssuesEvent',
                  'MemberEvent',
                  'PublicEvent',
                  'PullRequestEvent',
                  'PullRequestReviewEvent',
                  'PullRequestReviewCommentEvent',
                  'PullRequestReviewThreadEvent',
                  'PushEvent',
                  'ReleaseEvent',
                  'SponsorshipEvent',
                  'WatchEvent']
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 4
            }
        }
    }
    for index_name in index_list:
        index_name_lowercase = get_index_name(index_name)
        print(index_name_lowercase)
        # response = client.indices.create(index_name_lowercase, body=index_body)
        # client.indices.delete(index=index_name_lowercase)


def delete_index():
    index_list = ['CommitCommentEvent',
                  'CreateEvent',
                  'DeleteEvent',
                  'ForkEvent',
                  'GollumEvent',
                  'IssueCommentEvent',
                  'IssuesEvent',
                  'MemberEvent',
                  'PublicEvent',
                  'PullRequestEvent',
                  'PullRequestReviewEvent',
                  'PullRequestReviewCommentEvent',
                  'PullRequestReviewThreadEvent',
                  'PushEvent',
                  'ReleaseEvent',
                  'SponsorshipEvent',
                  'WatchEvent']
    for index_name in index_list:
        index_name_lowercase = get_index_name(index_name)
        print(index_name_lowercase)
        # response = client.indices.create(index_name_lowercase, body=index_body)
        # client.indices.delete(index=index_name_lowercase)


def get_index_name(index_name):
    result = index_name[0].lower()
    for i in range(1, len(index_name)):
        if index_name[i].isupper():
            result = result + '_' + index_name[i].lower()
        else:
            result = result + index_name[i]
    return result

# if __name__ == '__main__':
#     # file_name_pre = '2019-01-01-'
#     # file_name_end = '.json.gz'
#     # for j in range(1, 32):
#     #     if j < 10:
#     #         j = '0' + str(j)
#     #     for i in range(0, 24):
#     #         file_name = f"2019-01-{j}-{i}.json.gz"
#     #         # print(file_name)
#     #         un_gzip(file_name)
#     #
#
#     for j in range(1, 32):
#         if j < 10:
#             j = '0' + str(j)
#         for i in range(0, 24):
#             file_name = f"2019-01-{j}-{i}.json"
#             parse_json_data(file_name)
#     # file_name_end = '.json'
# for i in range(0, 24):
#     file_name = file_name_pre + str(i) + file_name_end
#     get_json(file_name)
# create_index()
# delete_index()
