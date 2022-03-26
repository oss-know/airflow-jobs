#!/bin/bash
SRC_HOST=192.168.8.10:9200
DST_HOST=192.168.8.108:19201
indice=(gits)
#indice=(gits github_commits github_issues github_pull_requests github_issues_comments github_issues_timeline github_profile)
#indice=(gits maillists github_commits github_issues github_pull_requests github_issues_comments github_issues_timeline github_profile check_sync_data)
for index in "${indice[@]}"
do
	echo $index

	docker run --rm -e NODE_TLS_REJECT_UNAUTHORIZED=0 -v $(pwd)/:/tmp elasticdump/elasticsearch-dump \
#	--searchBody="{\"query\":{\"match\":{\"search_key.owner.keyword\":\"ClickHouse\"}}}" \
									 --input=https://admin:admin@${SRC_HOST}/${index} \
									 --output=https://admin:admin@${DST_HOST}/${index} \
									 --limit=5000 \
									 --type=data
done

#--searchBody="{\"query\":{\"match\":{\"search_key.owner.keyword\":\"ClickHouse\"}}}" \
