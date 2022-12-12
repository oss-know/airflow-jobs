# -*-coding:utf-8-*-
import time
import csv
from datetime import datetime
from oss_know.libs.util.log import logger
from clickhouse_driver import Client, connect


class CKServer:
    def __init__(self, host, port, user, password, database, settings={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()


def get_clickhouse_data(ck, owner, repo, top_dir_level2, limit):
    sql = f"""
select search_key__owner ,
    search_key__repo ,
    dir_level2 , 
    author_email,
    author_tz,
    sum(`files.insertions`) as total_insertions,
    sum(`files.deletions`) as total_deletions,
    sum(`files.lines`) as total_lines,
    COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email ,
        author_tz ,
        `files.file_name` ,
            `files.insertions`,
            `files.deletions`,
            `files.lines` ,
        splitByChar('/',`files.file_name`) as dir_list,
        arrayStringConcat(arraySlice(dir_list, 1,2),'/') as dir_level2 
    from gits 
    array join `files.file_name` ,
        `files.insertions`,
        `files.deletions`,
        `files.lines` 
    where dir_level2 GLOBAL in ('{top_dir_level2}',)
        and search_key__owner = '{owner}'
        and search_key__repo = '{repo}'
)
group by search_key__owner, search_key__repo,
    dir_level2, author_email, author_tz
order by alter_file_count desc 
-- order by dir_level2 asc, alter_file_count desc 
LIMIT {limit}
"""
    # print(sql)

    return ck.execute_no_params(sql)


def get_top_n_email(ck, owner, repo, top_dir_level2, limit):
    sql = f"""
    select search_key__owner, search_key__repo,dir_level2,author_email ,sum(alter_file_count) as total_alter_file_count from (select search_key__owner ,
    search_key__repo ,
    dir_level2 , 
    author_email,
    author_tz,
    sum(`files.insertions`) as total_insertions,
    sum(`files.deletions`) as total_deletions,
    sum(`files.lines`) as total_lines,
    COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email ,
        author_tz ,
        `files.file_name` ,
            `files.insertions`,
            `files.deletions`,
            `files.lines` ,
        splitByChar('/',`files.file_name`) as dir_list,
        arrayStringConcat(arraySlice(dir_list, 1,2),'/') as dir_level2 
    from gits 
    array join `files.file_name` ,
        `files.insertions`,
        `files.deletions`,
        `files.lines` 
    where dir_level2 GLOBAL in ('{top_dir_level2}',)
        and search_key__owner = '{owner}'
        and search_key__repo = '{repo}'
)
group by search_key__owner, search_key__repo,
    dir_level2, author_email, author_tz
order by alter_file_count desc 
-- order by dir_level2 asc, alter_file_count desc 
LIMIT {limit}) group by search_key__owner,search_key__repo,dir_level2,author_email order by total_alter_file_count desc

    """
    # print(sql)

    return ck.execute_no_params(sql)


def get_dir_level2(ck, owner, repo, limit, un_dir_list):
    un_dir_str = ''
    for un_dir_level2 in un_dir_list:
        un_dir_str += f" and dir_level2 not like '{un_dir_level2}'"
    print(un_dir_str)
    sql = f"""
    select search_key__owner,search_key__repo,
                dir_level2,
                COUNT(*) alter_files_count from (
                    select search_key__owner,
                        search_key__repo,
                        splitByChar('/', `files.file_name`)                as dir_list,
                        arrayStringConcat(arraySlice(dir_list, 1, 2), '/') as dir_level2
                     from gits
                         array join `files.file_name`
                        , `files.insertions`
                        , `files.deletions`
                        , `files.lines`
                     WHERE if_merged = 0
                       and files.file_name not like '%=>%'
                       and length(dir_list) >= 3
                       and search_key__owner = '{owner}'
                       and search_key__repo = '{repo}'
                       {un_dir_str}
                )
    group by search_key__owner, search_key__repo, dir_level2 order by alter_files_count desc limit {limit}
    """
    results = ck.execute_no_params(sql)
    dir_list_top = []
    for un_dir_level2 in results:
        dir_list_top.append(un_dir_level2[2])
    return dir_list_top


def get_tz_file_count(ck, owner, repo, dir_level2, email):
    sql = f"""
    select search_key__owner,
       search_key__repo,
       dir_level2,
       author_email,
       author_tz,
        sum(`files.insertions`) as total_insertions,
                    sum(`files.deletions`) as total_deletions,
                    sum(`files.lines`) as total_lines,
       count() as counts
       from(
           select search_key__owner,
               search_key__repo,
               author_email,
               author_tz,
                   `files.insertions`,
        `files.deletions`,
        `files.lines` ,
               `files.file_name`,
               splitByChar('/', `files.file_name`) as dir_list,
               arrayStringConcat(arraySlice(dir_list, 1, 2), '/') as dir_level2
           from gits
           array join `files.file_name`,
           `files.insertions`,
        `files.deletions`,
        `files.lines`
           where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and dir_level2 = '{dir_level2}'
              and author_email = '{email}'
           )
group by search_key__owner,
         search_key__repo,
         dir_level2,
         author_email,
         author_tz order by counts desc"""

    return ck.execute_no_params(sql)


def get_file_count_p_people(ck, owner, repo, dir_level2, email):
    sql = f"""
    select search_key__owner,
       search_key__repo,
       dir_level2,
       author_email,
       count() as counts
       from(
           select search_key__owner,
               search_key__repo,
               author_email,
               author_tz,
                   `files.insertions`,
        `files.deletions`,
        `files.lines` ,
               `files.file_name`,
               splitByChar('/', `files.file_name`) as dir_list,
               arrayStringConcat(arraySlice(dir_list, 1, 2), '/') as dir_level2
           from gits
           array join `files.file_name`,
           `files.insertions`,
        `files.deletions`,
        `files.lines`
           where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and dir_level2 = '{dir_level2}'
              and author_email = '{email}'
           )
group by search_key__owner,
         search_key__repo,
         dir_level2,
         author_email
         order by counts desc"""

    return ck.execute_no_params(sql)


def get_dir2_all_commit_count(ck, owner, repo, dir_level2):
    sql = f"""
    select search_key__owner ,
    search_key__repo ,
    dir_level2 , 
    COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email ,
        author_tz ,
        `files.file_name` ,
            `files.insertions`,
            `files.deletions`,
            `files.lines` ,
        splitByChar('/',`files.file_name`) as dir_list,
        arrayStringConcat(arraySlice(dir_list, 1,2),'/') as dir_level2 
    from gits 
    array join `files.file_name` ,
        `files.insertions`,
        `files.deletions`,
        `files.lines` 
    where dir_level2 = '{dir_level2}'
        and search_key__owner = '{owner}'
        and search_key__repo = '{repo}'
)
group by search_key__owner, search_key__repo,
    dir_level2
    """
    return ck.execute_no_params(sql)


def get_tz_count(ck, owner, repo, dir_level2):
    sql = f"""
    select search_key__owner ,
    search_key__repo ,
    dir_level2 , 
    author_tz,
    COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email ,
        author_tz ,
        `files.file_name` ,
            `files.insertions`,
            `files.deletions`,
            `files.lines` ,
        splitByChar('/',`files.file_name`) as dir_list,
        arrayStringConcat(arraySlice(dir_list, 1,2),'/') as dir_level2 
    from gits 
    array join `files.file_name` ,
        `files.insertions`,
        `files.deletions`,
        `files.lines` 
    where dir_level2 = '{dir_level2}'
        and search_key__owner = '{owner}'
        and search_key__repo = '{repo}'
)
group by search_key__owner, search_key__repo,
    dir_level2,author_tz
order by alter_file_count desc 
    """
    return ck.execute_no_params(sql)


def get_level_2_dir_domain_count(ck, owner, repo, dir_level2):
    sql = f"""
    select search_key__owner ,
    search_key__repo ,
    dir_level2 , 
    email_domain,
    COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        splitByChar('@',`author_email`)[2] as email_domain,
        author_tz ,
        `files.file_name` ,
            `files.insertions`,
            `files.deletions`,
            `files.lines` ,
        splitByChar('/',`files.file_name`) as dir_list,
        arrayStringConcat(arraySlice(dir_list, 1,2),'/') as dir_level2 
    from gits 
    array join `files.file_name` ,
        `files.insertions`,
        `files.deletions`,
        `files.lines` 
    where dir_level2 = '{dir_level2}'
        and search_key__owner = '{owner}'
        and search_key__repo = '{repo}'
)
group by search_key__owner, search_key__repo,
    dir_level2,email_domain ORDER by alter_file_count desc limit 20
    """

    return ck.execute_no_params(sql)


def get_profile_by_email(ck, email):
    sql = f"""select location,company ,
    inferred_from_location__country                     ,
    inferred_from_location__locality
 from github_profile
 where id != 0
   and id = (select distinct author__id
             from github_commits
             where commit__author__email = '{email}'
               and author__id != 0)
order by search_key__updated_at desc limit 1"""
    # print(sql)
    return ck.execute_no_params(sql=sql)


def get_dir2_contribute_data(ck_conn_info, project_info):
    ck = CKServer(host=ck_conn_info["HOST"],
                  port=ck_conn_info["PORT"],
                  user=ck_conn_info["USER"],
                  password=ck_conn_info["PASSWD"],
                  database=ck_conn_info["DATABASE"])

    project_line = project_info["project_line"]
    owner = project_info["owner"]
    repo = project_info["repo"]
    start_time = int(time.time())
    top_dir_limit = 10
    dir_developer_limit = 20
    # 'vendor/%', 'test/%', 'Godeps/%', 'third_party/%', 'docs/%', 'contrib/%'
    un_dir_list = []

    # 获取top n 二级目录
    top_n_dir_list = get_dir_level2(ck=ck,
                                    owner=owner, repo=repo,
                                    limit=top_dir_limit,
                                    un_dir_list=un_dir_list)

    bulk_list = []
    for now_top_dir in top_n_dir_list:
        domain_distribution = ""
        dir2_all_alter_count = \
            get_dir2_all_commit_count(ck=ck, owner=owner, repo=repo,
                                      dir_level2=now_top_dir)[0][3]

        level_2_dir_domain_count = get_level_2_dir_domain_count(ck=ck, owner=owner,
                                                                repo=repo,
                                                                dir_level2=now_top_dir)
        for i in level_2_dir_domain_count:
            domain_distribution += f"{i[3]}:{format(i[4] / dir2_all_alter_count, '.1%')} | "
        results1 = get_tz_count(ck=ck, owner=owner, repo=repo,
                                dir_level2=now_top_dir)
        tz_distribution = ""
        for result in results1:
            if result[3] > 0:
                tz_distribution += f"+{result[3]}:{format(result[4] / dir2_all_alter_count, '.1%')}".ljust(10,
                                                                                                           ' ') + '| '
            else:
                tz_distribution += f"{result[3]}:{format(result[4] / dir2_all_alter_count, '.1%')}".ljust(10,
                                                                                                          ' ') + '| '
        # print(tz_distribution)
        results = get_top_n_email(ck=ck, owner=owner, repo=repo,
                                  top_dir_level2=now_top_dir,
                                  limit=dir_developer_limit)
        # print(results)
        for dir_email in results:
            row_dict = {
                        "ck_data_insert_at": int(time.time()*1000),
                        "department": project_line,
                        "owner": owner,
                        "repo": repo,
                        "dir_level2": dir_email[2],
                        "domain_distribution": domain_distribution,
                        "tz_distribution": tz_distribution,
                        "email": dir_email[3],
                        "sum_tz_commit_files_count": 0,
                        "tz_commit_files_count": "",
                        "profile_location": "",
                        "profile_company": "",
                        "inferred_from_location_country_or_region": "",
                        "inferred_from_location_locality_or_region": ""
                        }
            per_people_datas = get_tz_file_count(ck=ck,
                                                 owner=dir_email[0],
                                                 repo=dir_email[1],
                                                 dir_level2=dir_email[2],
                                                 email=dir_email[3])
            row_dict["sum_tz_commit_files_count"] = get_file_count_p_people(ck=ck,
                                                                            owner=dir_email[0],
                                                                            repo=dir_email[1],
                                                                            dir_level2=dir_email[2],
                                                                            email=dir_email[3])[0][4]
            for per_people_data in per_people_datas:
                # print(per_people_data)

                if per_people_data[4] > 0:
                    row_dict["tz_commit_files_count"] = row_dict[
                                                            "tz_commit_files_count"] + f"+{per_people_data[4]}:{format(per_people_data[8] / row_dict['sum_tz_commit_files_count'], '.1%')}".ljust(
                        10, ' ') + '| '

                else:
                    row_dict["tz_commit_files_count"] = row_dict[
                                                            "tz_commit_files_count"] + f"{per_people_data[4]}:{format(per_people_data[8] / row_dict['sum_tz_commit_files_count'], '.1%')}".ljust(
                        10, ' ') + '| '
            profile_data = get_profile_by_email(ck=ck, email=dir_email[3])
            if profile_data:
                row_dict["profile_location"] = profile_data[0][0].replace(',', ' ')
                row_dict["profile_company"] = profile_data[0][1].replace(',', ' ')
                row_dict["inferred_from_location_country_or_region"] = profile_data[0][2].replace(',', ' ')
                row_dict["inferred_from_location_locality_or_region"] = profile_data[0][3].replace(',', ' ')
            # print(row_dict)
            logger.info(row_dict)

            bulk_list.append(row_dict)

        # logger.info(f"owner: {owner_repo['owner']}  repo: {owner_repo['repo']} has been inserted")
    insert_sql = f"insert into dir2_analysis_v1 values"
    ck.execute(insert_sql, bulk_list)
    end_time = int(time.time())
    logger.info(f"analysis spend time {(end_time - start_time) / 60} min-----------------------")
    ck.close()

