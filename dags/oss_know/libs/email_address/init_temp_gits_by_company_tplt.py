import copy

from loguru import logger

from oss_know.libs.email_address.temp_gits_by_company_tplt import TEMP_GITS_BY_COMPANY_DEFAULT_TPLT
from oss_know.libs.util.base import get_clickhouse_client


def load_all_temp_gits_by_company(clickhouse_server_info, owners):
    ck = get_clickhouse_client(clickhouse_server_info)
    truncate_local_sql = "truncate table temp_gits_by_company_local"
    truncate_sql = "truncate table temp_gits_by_company"
    ck.execute_no_params(truncate_local_sql)
    ck.execute_no_params(truncate_sql)
    values_to_insert = []
    github_id_login_pair = {}
    check_email_dict = {}
    gits_values = []
    if not owners:
        gits_sql = "SELECT DISTINCT * FROM gits"
        gits_values.extend(ck.execute_no_params(gits_sql))
    else:
        for owner in owners:
            gits_sql = f"select * from gits where search_key__owner = '{owner}'"
            gits_values.extend(ck.execute_no_params(gits_sql))
    gits_values_len = len(gits_values)
    get_gits_table_columns_sql = "select distinct name from system.columns where database = 'default' AND table = 'gits'"
    gits_columns = ck.execute_no_params(get_gits_table_columns_sql)
    gits_columns_len = len(gits_columns)
    total_count = gits_values_len
    page_count = 5000
    total_page = total_count // page_count
    if total_count % page_count:
        total_page = total_page + 1
    count = 1
    page_num = 1
    for values_index in range(0, gits_values_len):
        value = copy.deepcopy(TEMP_GITS_BY_COMPANY_DEFAULT_TPLT)
        gits_value = gits_values[values_index]
        for index in range(0, gits_columns_len):
            value[gits_columns[index][0]] = gits_value[index]
        author_email = gits_value[10]
        committer_email = gits_value[12]
        if author_email:
            value = add_email_column(author_email, value, check_email_dict, ck, github_id_login_pair, "author")
        if committer_email:
            value = add_email_column(author_email, value, check_email_dict, ck, github_id_login_pair, "committer")
        values_to_insert.append(value)
        if (page_num < total_page and count < page_count) or (
                page_num == total_page and count < total_count - (page_num - 1) * page_count):
            count = count + 1
        else:
            page_num = page_num + 1
            count = 1
            insert_temp_gits_by_company_sql = "INSERT INTO temp_gits_by_company (*) VALUES"
            ck.execute(insert_temp_gits_by_company_sql, values_to_insert)
            values_to_insert = []
    if values_to_insert:
        insert_temp_gits_by_company_sql = "INSERT INTO temp_gits_by_company (*) VALUES"
        ck.execute(insert_temp_gits_by_company_sql, values_to_insert)


def add_email_column(email, value, check_email_dict, ck, github_id_login_pair, pre):
    split_result = email.split("@")
    split_result_len = len(split_result)
    if split_result_len == 2:
        value[pre + "_email_account"] = split_result[0]
        value[pre + "_email_domain"] = split_result[1]
    elif split_result_len == 1:
        if email.startswith("@"):
            value[pre + "_email_account"] = split_result[0]
        else:
            value[pre + "_email_domain"] = split_result[0]
    if email in check_email_dict:
        value[pre + "_github_id"] = list(check_email_dict[email])[0]
        value[pre + "_github_login"] = check_email_dict[email][value[pre + "_github_id"]]
        print("第二次取值：", value[pre + "_github_id"], value[pre + "_github_login"])
    else:
        github_profile_sql = f"select id , login from github_profile where email = '{email}'"
        profiles = ck.execute_no_params(github_profile_sql)
        if profiles:
            profile_id = profiles[0][0]
            profile_login = profiles[0][1]
            github_id_login_pair[profile_id] = profile_login
            check_email_dict[email] = github_id_login_pair
            value[pre + "_github_id"] = profile_id
            value[pre + "_github_login"] = profile_login
            print("第一次取值，", value[pre + "_github_id"], value[pre + "_github_login"])
    return value

    ck.close()
