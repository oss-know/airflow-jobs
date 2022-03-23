import datetime

from sqlalchemy import false

TEMP_GITS_BY_COMPANY_DEFAULT_TPLT = {
    "author_email_account": "",
    "author_email_domain": "",
    "author_github_login": "",
    "author_github_id": 0,
    "committer_email_account": "",
    "committer_email_domain": "",
    "committer_github_login": "",
    "committer_github_id": 0,
    "search_key": {
        "owner": "",
        "repo": "",
        "origin": "",
        "updated_at": 0
    },
    "raw_data": {
        "message": "",
        "hexsha": "",
        "parents": [
            "",
            ""
        ],
        "author_tz": 0,
        "committer_tz": 0,
        "author_name": "",
        "author_email": "",
        "committer_name": "",
        "committer_email": "",
        "authored_date": datetime.datetime.strptime("1970-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        "authored_timestamp": 0,
        "committed_date": datetime.datetime.strptime("1970-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        "committed_timestamp": 0,
        "files": [
            {
                "insertions": 0,
                "deletions": 0,
                "lines": 0,
                "file_name": ""
            },
            {
                "insertions": 0,
                "deletions": 0,
                "lines": 0,
                "file_name": ""
            }
        ],
        "total": {
            "insertions": 0,
            "deletions": 0,
            "lines": 0,
            "files": 0
        },
        "if_merged": false,
        "type": ""
    }
}
