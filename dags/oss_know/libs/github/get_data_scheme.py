from oss_know.libs.util.base import get_opensearch_client
from opensearchpy import helpers as opensearch_helpers

OPENSEARCH_GIT_RAW = "gits"
OPENSEARCH_INDEX_GITHUB_COMMITS = "github_commits"
OPENSEARCH_INDEX_GITHUB_ISSUES = "github_issues"
OPENSEARCH_INDEX_GITHUB_ISSUES_COMMENTS = "github_issues_comments"
OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE = "github_issues_timeline"
OPENSEARCH_INDEX_GITHUB_PULL_REQUESTS = "github_pull_requests"
OPENSEARCH_INDEX_GITHUB_PROFILE = "github_profile"
GITHUB_ISSUES_TIMELINE_EVENT = ["added_to_project", "assigned", "automatic_base_change_failed",
                                "automatic_base_change_succeeded", "base_ref_changed", "closed", "commented",
                                "committed", "connected", "convert_to_draft", "converted_note_to_issue",
                                "cross-referenced", "demilestoned", "deployed", "deployment_environment_changed",
                                "disconnected", "head_ref_deleted", "head_ref_restored", "labeled", "locked",
                                "mentioned", "marked_as_duplicate", "merged", "milestoned", "moved_columns_in_project",
                                "pinned", "ready_for_review", "referenced", "removed_from_project", "renamed",
                                "reopened", "review_dismissed", "review_requested", "review_request_removed",
                                "reviewed", "subscribed", "transferred", "unassigned", "unlabeled", "unlocked",
                                "unmarked_as_duplicate", "unpinned", "nsubscribed", "user_blocked"]


def get_data_scheme(opensearch_conn_info):
    opensearch_client = get_opensearch_client(opensearch_conn_info)
    os_source = OPENSEARCH_INDEX_GITHUB_ISSUES_TIMELINE
    data_result = {}
    for event in GITHUB_ISSUES_TIMELINE_EVENT:
        datass = opensearch_helpers.scan(opensearch_client,
                                         scroll="30m",
                                         preserve_order=True,
                                         index=os_source,
                                         query={
                                             "query": {
                                                 "term": {
                                                     "raw_data.event": {
                                                         "value": event
                                                     }
                                                 }
                                             }
                                         },
                                         doc_type="_doc"

                                         )
        data_set = set()
        for data in datass:
            if data["_source"]['raw_data']:
                data_dict = data["_source"]['raw_data']
                data_set = data_set | set(data_dict.keys())
        data_result.update({os_source + "::" + event: data_set})
        print(os_source, "中event为", event, "有这些字段：  ", data_result[os_source + "::" + event])

    event_no_set = set()
    count = 0
    for k, v in data_result.items():
        if not v:
            count = count + 1
            event_no_set.add(k.split("::")[1])

    print(os_source, "中没有数据的event：  ", event_no_set,", 共",count,"个")
