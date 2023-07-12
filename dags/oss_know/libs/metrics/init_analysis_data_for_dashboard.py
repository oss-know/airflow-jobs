# -*-coding:utf-8-*-
import time

from oss_know.libs.util.base import now_timestamp
from oss_know.libs.util.clickhouse_driver import CKServer


def get_dir_n(owner, repo, ck_con):

    ck_client = CKServer(host=ck_con['HOST'], port=ck_con['PORT'], user=ck_con['USER'], password=ck_con['PASSWD'],
                   database=ck_con['DATABASE'])

    results = ck_client.execute_no_params(f"""
    SELECT search_key__owner,
       search_key__repo,
        author_tz,
        committer_tz,
            author_name,
            author_email,
            authored_date,
            committer_name,
            committer_email,
            committed_date,
              splitByChar('/', `files.file_name`)                as dir_list,
              arraySlice(dir_list, 1, -1) as array_slice,
              arrayStringConcat(array_slice, '/') as dir_level_n
           FROM gits
               array join `files.file_name`
           WHERE 
           search_key__owner='{owner}'
            and search_key__repo='{repo}' and
            if_merged = 0
             AND files.file_name not like '%=>%'
             AND length(dir_list) >= 2
    """)
    bulk_data = []
    count = 0
    dir_set = set()
    """
    full_dir_dict:目录的完整结构 /a/b/c
    level_n_dir_dict：完整结构下每层目录结构 /a /a/b
    """
    for result in results:
        dir_list = result[-2]
        dir_1 = dir_list[0] + '/'
        dir_set.add(dir_1)
        full_dir_dict = {}
        full_dir_dict['ck_data_insert_at'] = now_timestamp()
        full_dir_dict['search_key__owner'] = result[0]
        full_dir_dict['search_key__repo'] = result[1]
        full_dir_dict['author_tz'] = result[2]
        full_dir_dict['committer_tz'] = result[3]
        full_dir_dict['author_name'] = result[4]
        full_dir_dict['author_email'] = result[5]
        full_dir_dict['authored_date'] = result[6]
        full_dir_dict['committer_name'] = result[7]
        full_dir_dict['committer_email'] = result[8]
        full_dir_dict['committed_date'] = result[9]
        full_dir_dict['dir_list'] = result[10]
        full_dir_dict['array_slice'] = result[11]
        full_dir_dict['dir_level_n'] = result[12]
        full_dir_dict['in_dir'] = dir_1
        bulk_data.append(full_dir_dict)
        count += 1
        for i in range(1, len(dir_list)):
            dir_1 = dir_1 + dir_list[i] + '/'
            dir_set.add(dir_1)
            level_n_dir_dict = {}
            level_n_dir_dict['ck_data_insert_at'] = now_timestamp()
            level_n_dir_dict['search_key__owner'] = result[0]
            level_n_dir_dict['search_key__repo'] = result[1]
            level_n_dir_dict['author_tz'] = result[2]
            level_n_dir_dict['committer_tz'] = result[3]
            level_n_dir_dict['author_name'] = result[4]
            level_n_dir_dict['author_email'] = result[5]
            level_n_dir_dict['authored_date'] = result[6]
            level_n_dir_dict['committer_name'] = result[7]
            level_n_dir_dict['committer_email'] = result[8]
            level_n_dir_dict['committed_date'] = result[9]
            level_n_dir_dict['dir_list'] = result[10]
            level_n_dir_dict['array_slice'] = result[11]
            level_n_dir_dict['dir_level_n'] = result[12]
            level_n_dir_dict['in_dir'] = dir_1
            bulk_data.append(level_n_dir_dict)
            count += 1

        if len(bulk_data) > 50000:
            insert_sql = 'insert into table gits_dir_label values'
            response = ck_client.execute(insert_sql, bulk_data)
            print(F"INSERT INSERT TABLE *** {response}")
            bulk_data.clear()
    if bulk_data:
        insert_sql = 'insert into table gits_dir_label values'
        response = ck_client.execute(insert_sql, bulk_data)
        print(F"INSERT INSERT TABLE *** {response}")
    all_dir = list(dir_set)
    bulk_dir_list = []
    for i in all_dir:
        dir_dict = {"search_key__owner": owner,
                    "search_key__repo": repo,
                    "dir": i,
                    "ck_data_insert_at": now_timestamp()
                    }
        bulk_dir_list.append(dir_dict)
    if bulk_dir_list:
        insert_sql = 'insert into table gits_dir values'
        ck_client.execute(insert_sql, bulk_dir_list)
    ck_client.close()


def get_alter_files_count(ck_con, owner='', repo=''):
    ck = CKServer(host=ck_con['HOST'], port=ck_con['PORT'], user=ck_con['USER'], password=ck_con['PASSWD'],
                  database=ck_con['DATABASE'])
    if owner == '' and repo == '':

        sql = f"""
        select * from (select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '北美' as area,
        COUNT() alter_file_count
    from (
        select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where
             author_tz global in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '欧洲西部' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where
    
            author_tz global in (0,1,2)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '欧洲东部' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where author_tz global in (3,4)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '印度' as area,
        COUNT() alter_file_count
    from (
        select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where author_tz global in (5)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '中国' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where author_tz global in (8)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '日韩' as area,
        COUNT() alter_file_count
    from (
        select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where author_tz global in (9)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '澳洲' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where author_tz global in (10)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date) order by search_key__owner;
        """
    else:
        sql = f"""
        select * from (select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '北美' as area,
        COUNT() alter_file_count
    from (
        select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where
             search_key__owner = '{owner}' and search_key__repo = '{repo}' and
             author_tz global in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '欧洲西部' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
            author_tz global in (0,1,2)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '欧洲东部' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
        
        where 
        search_key__owner = '{owner}' and search_key__repo = '{repo}' and
        author_tz global in (3,4)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '印度' as area,
        COUNT() alter_file_count
    from (
        select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where 
        search_key__owner = '{owner}' and search_key__repo = '{repo}' and
        author_tz global in (5)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '中国' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where 
        search_key__owner = '{owner}' and search_key__repo = '{repo}' and
        author_tz global in (8)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '日韩' as area,
        COUNT() alter_file_count
    from (
        select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where 
        search_key__owner = '{owner}' and search_key__repo = '{repo}' and
        author_tz global in (9)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date
    
    union all
    
    select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        '澳洲' as area,
        COUNT() alter_file_count
    from (
         select search_key__owner,
            search_key__repo,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from  gits_dir_label
    
        where 
        search_key__owner = '{owner}' and search_key__repo = '{repo}' and
        author_tz global in (10)
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date) order by search_key__owner;
        """
    results = ck.execute_no_params(sql)
    bulk_data = []
    for result in results:
        data_dict = {}
        data_dict["search_key__owner"] = result[0]
        data_dict["search_key__repo"] = result[1]
        data_dict["in_dir"] = result[2]
        data_dict["authored_date"] = result[3]
        data_dict["area"] = result[4]
        data_dict["alter_file_count"] = result[5]
        data_dict["ck_data_insert_at"] = now_timestamp()
        bulk_data.append(data_dict)
        if len(bulk_data) > 20000:
            response = ck.execute("insert into table gits_alter_file_times values", bulk_data)
            print(f"insert into table gits_alter_file_times {response} ")
            bulk_data.clear()
    if bulk_data:
        response = ck.execute("insert into table gits_alter_file_times values", bulk_data)
        print(f"insert into table gits_alter_file_times {response} ")
    ck.close()


def get_dir_contributer_count(ck_con, owner='', repo=''):
    ck = CKServer(host=ck_con['HOST'], port=ck_con['PORT'], user=ck_con['USER'], password=ck_con['PASSWD'],
                  database=ck_con['DATABASE'])
    if owner == '' and repo == '':
        sql = f"""
    select * from (select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count 
    from (select search_key__owner ,
    search_key__repo ,
    in_dir ,
    authored_date,
    '北美' as area,
    author_email
--     COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email,
        toYYYYMM(authored_date) as authored_date,
        in_dir
    from gits_dir_label
    where
author_tz global in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12)
)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,author_email)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,area

union all

select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
search_key__owner ,
    search_key__repo ,
    in_dir ,
    authored_date,
    '欧洲西部' as area,
    author_email
--     COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email,
        toYYYYMM(authored_date) as authored_date,
        in_dir
    from gits_dir_label
    where
         author_tz global in (0,1,2)
)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,author_email)
group by search_key__owner, search_key__repo,
    in_dir,area,authored_date

union all

select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
search_key__owner ,
    search_key__repo ,
    in_dir ,
    authored_date,
    '欧洲东部' as area,
    author_email
--     COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email,
        toYYYYMM(authored_date) as authored_date,
        in_dir
    from gits_dir_label
    where
         author_tz global in (3,4)
)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,author_email)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,area

union all

select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
search_key__owner ,
    search_key__repo ,
    in_dir ,
    authored_date,
    '印度' as area,
    author_email
--     COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email,
        toYYYYMM(authored_date) as authored_date,
        in_dir
    from gits_dir_label
    where author_tz global in (5)
)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,author_email)
group by search_key__owner, search_key__repo,
    in_dir,area,authored_date

union all

select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
search_key__owner ,
    search_key__repo ,
    in_dir ,
    authored_date,
    '中国' as area,
    author_email
--     COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email,
        toYYYYMM(authored_date) as authored_date,
        in_dir
    from gits_dir_label
    where author_tz global in (8)
)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,author_email)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,area

union all

select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
search_key__owner ,
    search_key__repo ,
    in_dir ,
    authored_date,
    '日韩' as area,
    author_email
--     COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email,
        toYYYYMM(authored_date) as authored_date,
        in_dir
    from gits_dir_label
    where author_tz global in (9)
)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,author_email)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,area

union all

select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
search_key__owner ,
    search_key__repo ,
    in_dir ,
    authored_date,
    '澳洲' as area,
    author_email
--     COUNT() alter_file_count
from (
    select search_key__owner,
        search_key__repo,
        author_email,
        toYYYYMM(authored_date) as authored_date,
        in_dir
    from gits_dir_label
    where author_tz global in (10)
)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,author_email)
group by search_key__owner, search_key__repo,
    in_dir,authored_date,area) order by search_key__owner
    
    """
    else:
        sql = f"""
            select * from (select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as 
            contributor_count from (select search_key__owner ,
            search_key__repo ,
            in_dir ,
            authored_date,
            '北美' as area,
            author_email
        --     COUNT() alter_file_count
        from (
            select search_key__owner,
                search_key__repo,
                author_email,
                toYYYYMM(authored_date) as authored_date,
                in_dir
            from gits_dir_label
            where
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
        author_tz global in (-1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12)
        )
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,author_email)
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,area

        union all

        select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
        search_key__owner ,
            search_key__repo ,
            in_dir ,
            authored_date,
            '欧洲西部' as area,
            author_email
        --     COUNT() alter_file_count
        from (
            select search_key__owner,
                search_key__repo,
                author_email,
                toYYYYMM(authored_date) as authored_date,
                in_dir
            from gits_dir_label
            where
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
                 author_tz global in (0,1,2)
        )
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,author_email)
        group by search_key__owner, search_key__repo,
            in_dir,area,authored_date

        union all

        select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
        search_key__owner ,
            search_key__repo ,
            in_dir ,
            authored_date,
            '欧洲东部' as area,
            author_email
        --     COUNT() alter_file_count
        from (
            select search_key__owner,
                search_key__repo,
                author_email,
                toYYYYMM(authored_date) as authored_date,
                in_dir
            from gits_dir_label
            where
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
                 author_tz global in (3,4)
        )
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,author_email)
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,area

        union all

        select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
        search_key__owner ,
            search_key__repo ,
            in_dir ,
            authored_date,
            '印度' as area,
            author_email
        --     COUNT() alter_file_count
        from (
            select search_key__owner,
                search_key__repo,
                author_email,
                toYYYYMM(authored_date) as authored_date,
                in_dir
            from gits_dir_label
            where 
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
            author_tz global in (5)
        )
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,author_email)
        group by search_key__owner, search_key__repo,
            in_dir,area,authored_date

        union all

        select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
        search_key__owner ,
            search_key__repo ,
            in_dir ,
            authored_date,
            '中国' as area,
            author_email
        --     COUNT() alter_file_count
        from (
            select search_key__owner,
                search_key__repo,
                author_email,
                toYYYYMM(authored_date) as authored_date,
                in_dir
            from gits_dir_label
            where 
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
            author_tz global in (8)
        )
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,author_email)
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,area

        union all

        select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
        search_key__owner ,
            search_key__repo ,
            in_dir ,
            authored_date,
            '日韩' as area,
            author_email
        --     COUNT() alter_file_count
        from (
            select search_key__owner,
                search_key__repo,
                author_email,
                toYYYYMM(authored_date) as authored_date,
                in_dir
            from gits_dir_label
            where 
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
            author_tz global in (9)
        )
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,author_email)
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,area

        union all

        select search_key__owner,search_key__repo,in_dir,authored_date,area,count() as contributor_count from (select 
        search_key__owner ,
            search_key__repo ,
            in_dir ,
            authored_date,
            '澳洲' as area,
            author_email
        --     COUNT() alter_file_count
        from (
            select search_key__owner,
                search_key__repo,
                author_email,
                toYYYYMM(authored_date) as authored_date,
                in_dir
            from gits_dir_label
            where 
            search_key__owner = '{owner}' and search_key__repo = '{repo}' and
            author_tz global in (10)
        )
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,author_email)
        group by search_key__owner, search_key__repo,
            in_dir,authored_date,area) order by search_key__owner

            """
    results = ck.execute_no_params(sql)
    bulk_data = []
    for result in results:
        data_dict = {}
        data_dict["search_key__owner"] = result[0]
        data_dict["search_key__repo"] = result[1]
        data_dict["in_dir"] = result[2]
        data_dict["authored_date"] = result[3]
        data_dict["area"] = result[4]
        data_dict["contributer_count"] = result[5]
        data_dict["ck_data_insert_at"] = now_timestamp()
        bulk_data.append(data_dict)
        if len(bulk_data) > 20000:
            response = ck.execute("insert into table gits_dir_contributer values", bulk_data)
            print(f"insert into table gits_dir_contributer {response} ")
            bulk_data.clear()
    if bulk_data:
        response = ck.execute("insert into table gits_dir_contributer values", bulk_data)
        print(f"insert into table gits_dir_contributer {response} ")
    ck.close()


def get_alter_file_count_by_dir_email_domain(ck_con, owner='', repo=''):
    ck = CKServer(host=ck_con['HOST'], port=ck_con['PORT'], user=ck_con['USER'], password=ck_con['PASSWD'],
                  database=ck_con['DATABASE'])
    if owner == '' and repo == '':
        sql = f"""
        select * from (select search_key__owner ,
        search_key__repo ,
        in_dir ,
        authored_date,
        email_domain,
        COUNT() alter_file_count
    from (
        select search_key__owner,
            search_key__repo,
            multiIf(author_email='',
                if(author_name like '%@%',
                    splitByChar('@',`author_name`)[2],'empty_domain'),
                author_email like '%@%',
                splitByChar('@',`author_email`)[2],
                author_email like '%\%%',
                splitByChar('%',`author_email`)[2],
                author_email) as email_domain,
            toYYYYMM(authored_date) as authored_date,
            in_dir
        from gits_dir_label
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date,email_domain ) order by search_key__owner
    
        """
    else:
        sql = f"""
                select * from (select search_key__owner ,
                search_key__repo ,
                in_dir ,
                authored_date,
                email_domain,
                COUNT() alter_file_count
            from (
                select search_key__owner,
                    search_key__repo,
                    multiIf(author_email='',
                        if(author_name like '%@%',
                            splitByChar('@',`author_name`)[2],'empty_domain'),
                        author_email like '%@%',
                        splitByChar('@',`author_email`)[2],
                        author_email like '%\%%',
                        splitByChar('%',`author_email`)[2],
                        author_email) as email_domain,
                    toYYYYMM(authored_date) as authored_date,
                    in_dir
                from gits_dir_label where search_key__owner = '{owner}' and search_key__repo = '{repo}'
            )
            group by search_key__owner, search_key__repo,
                in_dir,authored_date,email_domain ) order by search_key__owner

                """
    results = ck.execute_no_params(sql)
    bulk_data = []
    for result in results:
        data_dict = {}
        data_dict["search_key__owner"] = result[0]
        data_dict["search_key__repo"] = result[1]
        data_dict["in_dir"] = result[2]
        data_dict["authored_date"] = result[3]
        data_dict["email_domain"] = result[4]
        data_dict["alter_file_count"] = result[5]
        data_dict["ck_data_insert_at"] = now_timestamp()
        bulk_data.append(data_dict)
        if len(bulk_data) > 20000:
            response = ck.execute("insert into table gits_dir_email_domain_alter_file_count values", bulk_data)
            print(f"insert into table gits_dir_email_domain_alter_file_count {response} ")
            bulk_data.clear()
    if bulk_data:
        response = ck.execute("insert into table gits_dir_email_domain_alter_file_count values", bulk_data)
        print(f"insert into table gits_dir_email_domain_alter_file_count {response} ")
    ck.close()


def get_contributer_by_dir_email_domain(ck_con, owner='', repo=''):
    ck = CKServer(host=ck_con['HOST'], port=ck_con['PORT'], user=ck_con['USER'], password=ck_con['PASSWD'],
                  database=ck_con['DATABASE'])
    if owner == '' and repo == '':
        sql = f"""
        select * from (select search_key__owner, search_key__repo,
        in_dir,authored_date,email_domain,count() contributor_count from (
        select search_key__owner,search_key__repo,email,authored_date,email_domain,in_dir from
        (select search_key__owner,
            search_key__repo,
    
            if(author_email='',
                if(author_name like '%@%',author_name,'empty_email'),
                author_email) as email,
            toYYYYMM(authored_date) as authored_date,
            multiIf(author_email='',
                if(author_name like '%@%',
                    splitByChar('@',`author_name`)[2],'empty_domain'),
                author_email like '%@%',
                splitByChar('@',`author_email`)[2],
                author_email like '%\%%',
                splitByChar('%',`author_email`)[2],
                author_email) as email_domain,
            in_dir
        from gits_dir_label)
        group by search_key__owner,search_key__repo,email,authored_date,email_domain,in_dir
    
    )
    group by search_key__owner, search_key__repo,
        in_dir,authored_date,email_domain
    order by contributor_count desc) order by search_key__owner
        """
    else:
        sql = f"""
                select * from (select search_key__owner, search_key__repo,
                in_dir,authored_date,email_domain,count() contributor_count from (
                select search_key__owner,search_key__repo,email,authored_date,email_domain,in_dir from
                (select search_key__owner,
                    search_key__repo,

                    if(author_email='',
                        if(author_name like '%@%',author_name,'empty_email'),
                        author_email) as email,
                    toYYYYMM(authored_date) as authored_date,
                    multiIf(author_email='',
                        if(author_name like '%@%',
                            splitByChar('@',`author_name`)[2],'empty_domain'),
                        author_email like '%@%',
                        splitByChar('@',`author_email`)[2],
                        author_email like '%\%%',
                        splitByChar('%',`author_email`)[2],
                        author_email) as email_domain,
                    in_dir
                from gits_dir_label where search_key__owner = '{owner}' and search_key__repo = '{repo}')
                group by search_key__owner,search_key__repo,email,authored_date,email_domain,in_dir

            )
            group by search_key__owner, search_key__repo,
                in_dir,authored_date,email_domain
            order by contributor_count desc) order by search_key__owner
                """
    results = ck.execute_no_params(sql)
    bulk_data = []
    for result in results:
        data_dict = {}
        data_dict["search_key__owner"] = result[0]
        data_dict["search_key__repo"] = result[1]
        data_dict["in_dir"] = result[2]
        data_dict["authored_date"] = result[3]
        data_dict["email_domain"] = result[4]
        data_dict["contributer_count"] = result[5]
        data_dict["ck_data_insert_at"] = now_timestamp()
        bulk_data.append(data_dict)
        if len(bulk_data) > 20000:
            response = ck.execute("insert into table gits_dir_email_domain_contributer_count values", bulk_data)
            print(f"insert into table gits_dir_email_domain_contributer_count {response} ")
            bulk_data.clear()
    if bulk_data:
        response = ck.execute("insert into table gits_dir_email_domain_contributer_count values", bulk_data)
        print(f"insert into table gits_dir_email_domain_contributer_count {response} ")
    ck.close()


def get_tz_distribution(ck_con, owner='', repo=''):
    ck = CKServer(host=ck_con['HOST'], port=ck_con['PORT'], user=ck_con['USER'], password=ck_con['PASSWD'],
                  database=ck_con['DATABASE'])
    if owner == '' and repo == '':
        sql = f"""
        select * from (select search_key__owner,search_key__repo,in_dir,author_email,sum(alter_files_count) 
        alter_files_count,groupArray(a) as tz_distribution
from (select search_key__owner,
             search_key__repo,
             in_dir,
             author_email,
             alter_files_count,
             map(author_tz, alter_files_count) as a
      from (select search_key__owner, search_key__repo,in_dir, author_email, author_tz, count() alter_files_count
            from (select search_key__owner,
                         search_key__repo,
                         author_email,
                         author_tz,
                         in_dir
                  from gits_dir_label

                  where
                    author_email != ''
                    )

            group by search_key__owner, search_key__repo, author_email, author_tz,in_dir
            order by alter_files_count desc))
group by search_key__owner,search_key__repo,in_dir,author_email
order by alter_files_count desc) order by search_key__owner
        """
    else:
        sql = f"""
                select * from (select search_key__owner,search_key__repo,in_dir,author_email,sum(alter_files_count) 
                alter_files_count,groupArray(a) as tz_distribution
        from (select search_key__owner,
                     search_key__repo,
                     in_dir,
                     author_email,
                     alter_files_count,
                     map(author_tz, alter_files_count) as a
              from (select search_key__owner, search_key__repo,in_dir, author_email, author_tz, 
              count() alter_files_count
                    from (select search_key__owner,
                                 search_key__repo,
                                 author_email,
                                 author_tz,
                                 in_dir
                          from gits_dir_label

                          where
                          search_key__owner = '{owner}' and search_key__repo = '{repo}' and
                            author_email != ''
                            )

                    group by search_key__owner, search_key__repo, author_email, author_tz,in_dir
                    order by alter_files_count desc))
        group by search_key__owner,search_key__repo,in_dir,author_email
        order by alter_files_count desc) order by search_key__owner
                """
    results = ck.execute_no_params(sql)
    bulk_data = []
    for result in results:
        data_dict = {}
        data_dict["search_key__owner"] = result[0]
        data_dict["search_key__repo"] = result[1]
        data_dict["in_dir"] = result[2]
        data_dict["author_email"] = result[3]
        data_dict["alter_files_count"] = result[4]
        data_dict["tz_distribution"] = result[5]
        data_dict["ck_data_insert_at"] = now_timestamp()
        bulk_data.append(data_dict)
        if len(bulk_data) > 20000:
            response = ck.execute("insert into table gits_dir_contributor_tz_distribution values", bulk_data)
            print(f"insert into table gits_dir_contributor_tz_distribution {response} ")
            bulk_data.clear()
    if bulk_data:
        response = ck.execute("insert into table gits_dir_contributor_tz_distribution values", bulk_data)
        print(f"insert into table gits_dir_contributor_tz_distribution {response} ")
    ck.close()
