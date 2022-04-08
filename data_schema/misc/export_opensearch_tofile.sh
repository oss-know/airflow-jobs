#!/bin/bash
#SRC_HOST=192.168.8.108:9200
DST_HOST=192.168.8.110:19201
#indice=(gits)
#indice=(github_commits)
indice=(github_commits github_issues github_pull_requests github_issues_comments github_issues_timeline maillists maillists_enriched)
#indice=(gits maillists github_commits github_issues github_pull_requests github_issues_comments github_issues_timeline github_profile check_sync_data)
for index in "${indice[@]}"
do
	echo $index

	docker run --rm -e NODE_TLS_REJECT_UNAUTHORIZED=0 -v $(pwd)/yangnan0406/:/tmp elasticdump/elasticsearch-dump \
									 --input=https://admin:admin@${DST_HOST}/${index} \
									 --output=/tmp/${index}.json.gzip \
									 --limit=5000 \
									 --type=data \
									 --fsCompress &
done

wait
