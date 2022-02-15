import datetime
from oss_know.libs.util.base import get_opensearch_client
from oss_know.libs.util.log import logger
from opensearchpy import helpers as opensearch_helpers
from oss_know.libs.base_dict.opensearch_index import OPENSEARCH_GIT_RAW, OPENSEARCH_INDEX_GITHUB_PROFILE, \
    OPENSEARCH_INDEX_EMAIL_ADDRESS


def load_email_address(opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info)
    load_email_address_from_gits = opensearch_helpers.scan(opensearch_client,
                                                    scroll="30m",
                                                    preserve_order=True,
                                                    index=OPENSEARCH_GIT_RAW,
                                                    query={
                                                        "query": {
                                                            "match_all": {}
                                                        },
                                                        "_source": ["search_key.owner", "search_key.repo",
                                                                    "raw_data.author_email", "raw_data.committer_email",
                                                                    "raw_data.committer_tz",
                                                                    "raw_data.committed_timestamp", "raw_data.hexsha"]
                                                    },
                                                    doc_type="_doc"
                                                    )
    email_address = set()
    for email_from_gits in load_email_address_from_gits:
        try:
            email_address.add(email_from_gits['_source']['raw_data']['author_email'])
            email_address.add(email_from_gits['_source']['raw_data']['committer_email'])
        except KeyError as e:
            logger.info(f"The key not exists in email_from_gits :{e}")
        except TypeError as e:
            logger.info(f"The value is null in email_from_gits :{e}")

    load_email_address_from_github_profile = opensearch_helpers.scan(opensearch_client,
                                                              scroll="30m",
                                                              preserve_order=True,
                                                              index=OPENSEARCH_INDEX_GITHUB_PROFILE,
                                                              query={
                                                                  "query": {
                                                                      "match_all": {}
                                                                  },
                                                                  "_source": ["raw_data.email", "raw_data.created_at",
                                                                              "raw_data.country_inferred_from_company",
                                                                              "raw_data.country_inferred_from_email_cctld",
                                                                              "raw_data.country_inferred_from_location",
                                                                              "raw_data.country_inferred_from_email_domain_company",
                                                                              "raw_data.final_company_inferred_from_company",
                                                                              "raw_data.final_company_inferred_from_company",
                                                                              "raw_data.inferred_from_location"],
                                                              },
                                                              doc_type="_doc"
                                                              )

    for email_from_github_profile in load_email_address_from_github_profile:
        try:
            email_address.add(email_from_github_profile['_source']['raw_data']['email'])
        except KeyError as e:
            logger.info(f"The key not exists in email_from_github_profile :{e}")
        except TypeError as e:
            logger.info(f"The value is null in email_from_github_profile :{e}")

    print("email_address如下:",email_address)

    # email_address_info = {
    #     "search_key": {
    #         "type": "email_address",
    #         'updated_at': int(datetime.datetime.now().timestamp() * 1000),
    #     },
    #     "raw_data": {
    #         "email": "",
    #         "country_inferred_from_cctld": "",
    #         "country_inferred_from_company": "",
    #         "company_inferred_from_domain": "",
    #         "git": [
    #             {
    #                 "owner": "",
    #                 "repo": "",
    #                 "commit_count": 0,
    #                 "commit_timezon_+8": 0,
    #                 "commit_timezon_-10": 0
    #             }
    #         ],
    #         "github": [],
    #         "gitee": [],
    #         "mailinglist": []
    #     }
    # }
    #
    # opensearch_client.index(index=OPENSEARCH_INDEX_EMAIL_ADDRESS,
    #                         body=email_address_info,
    #                         refresh=True)
