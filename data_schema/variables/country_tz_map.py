# -*-coding:utf-8-*-
import datetime
import csv

"""
create table country_tz_region_map_local on cluster replicated
(
    update_at DateTime64(3),
    country_or_region String,
    area String
)
    engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/country_tz_region_map_local', '{replica}')
        ORDER BY country_or_region
        SETTINGS index_granularity = 8192;


create table country_tz_region_map on cluster replicated as country_tz_region_map_local
    engine = Distributed('replicated', 'default', 'country_tz_region_map_local', rand());

"""
from clickhouse_server import CKServer

update_at_timestamp = int(datetime.datetime.now().timestamp() * 1000)
with open('country_tz_region_map.csv', 'r') as f:
    lines = csv.reader(f)
    bulk_data = []
    for i in lines:
        bulk_data.append({"update_at": update_at_timestamp, "country_or_region": i[0], 'area': i[1]})
    ck_client = CKServer(host='', port=0, user='', password='', database='')
    ck_client.execute('insert into table country_tz_region_map values', bulk_data)
