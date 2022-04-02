#!/bin/bash
#SRC_HOST=192.168.8.108:9200
DST_HOST=192.168.8.108:19201
#indice=(gits)
indice=(gits github_commits github_issues github_pull_requests github_issues_comments github_issues_timeline github_profile check_sync_data)
#indice=(gits maillists github_commits github_issues github_pull_requests github_issues_comments github_issues_timeline github_profile check_sync_data)
for index in "${indice[@]}"
do
	echo $index

	docker run --rm -e NODE_TLS_REJECT_UNAUTHORIZED=0 -v /home/fskhex/Desktop/10-opensearch-data/:/tmp elasticdump/elasticsearch-dump \
									 --input=/tmp/${index}.json.gzip \
									 --output=https://admin:admin@${DST_HOST}/${index} \
									 --limit=5000 \
									 --type=data \
									 --fsCompress &
done

wait