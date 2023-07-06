import json

from oss_know.libs.util.clickhouse import get_uniq_owner_repos

clickhouse_conn_info = json.loads('''
{
  "HOST": "192.168.8.60",
  "PASSWD": "default",
  "PORT": "19000",
  "USER": "default",
  "DATABASE": "default",
  "CLUSTER_NAME": "replicated"
}
''')

get_uniq_owner_repos(clickhouse_conn_info, 'gits')
