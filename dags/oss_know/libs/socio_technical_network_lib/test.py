import sys
sys.path.append("/root/jy/airflow-jobs/dags/")

from oss_know.libs.socio_technical_network_lib.abstract_func import abstract_func
from oss_know.libs.socio_technical_network_lib.interact_with_opensearch import get_commit_data_from_opensearch

owner = repo = "xmlpull-xpp3"
opensearch_conn_info = {"HOST": "123.57.177.158",
                        "PORT": "39012",
                        "USER": "admin",
                        "PASSWD": "admin"}

abstract_func(owner, repo, opensearch_conn_info)
# opensearch_data = get_commit_data_from_opensearch(owner, repo, opensearch_conn_info)
# for commit in opensearch_data:
#     print(commit)
#     break
