import copy
import datetime
from grimoire_elk.enriched.mbox import MBoxEnrich
from loguru import logger
from oss_know.libs.util.base import infer_country_from_emailcctld, infer_country_from_emaildomain, \
    infer_company_from_emaildomain, infer_country_from_company, get_clickhouse_client
from oss_know.libs.base_dict.clickhouse import CLICKHOUSE_EMAIL_ADDRESS, EMAIL_ADDRESS_SEARCH_KEY__UPDATED_AT, \
    EMAIL_ADDRESS_SEARCH_KEY__EMAIL, EMAIL_ADDRESS_EMIAL, EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILCCTLD, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILDOMAIN, EMAIL_ADDRESS_COMPANY_INFERRED_FROM_EMAIL, \
    EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY

from oss_know.libs.email_address.email_address_default_tplt import EMAIL_ADDRESS_DEFAULT_TPLT


def load_all_maillist_info(clickhouse_server_info):
    ck = get_clickhouse_client(clickhouse_server_info)
    init_email_address_dict = {}

    email_aliase_dict = {}
    mbox_helper = MBoxEnrich()
    maillists_enriched_sql = "select DISTINCT From,To from maillists_enriched"
    email_tuples = ck.execute_no_params(maillists_enriched_sql)
    for email_tuple in email_tuples:
        for origin_email_str in email_tuple:
            if origin_email_str:
                origin_email_list = origin_email_str.split(',')
                for origin_email in origin_email_list:
                    identity = mbox_helper.get_sh_identity(origin_email)
                    email = identity["email"]
                    email_aliase_dict[email] = identity["name"]
                    if email and '@' in email:
                        init_email_address_dict[email] = 0
    print(f"init_email_address_doit,{init_email_address_dict}")
    print(f"email_aliase_dict,{email_aliase_dict}")
    value = {}
    for k, v in init_email_address_dict.items():
        if k in email_aliase_dict:
            value["aliase"] = email_aliase_dict[k]
        else:
            value["aliase"] = ''
        print(value)

    ck.close()
