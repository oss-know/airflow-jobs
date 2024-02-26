CLICKHOUSE_RAW_DATA = "raw_data"

CLICKHOUSE_EMAIL_ADDRESS = 'email_address'
EMAIL_ADDRESS_SEARCH_KEY__UPDATED_AT = 'search_key__updated_at'
EMAIL_ADDRESS_SEARCH_KEY__EMAIL = 'search_key__email'
EMAIL_ADDRESS_EMIAL = 'email'
EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILCCTLD = 'country_inferred_from_emailcctld'
EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_EMAILDOMAIN = 'country_inferred_from_emaildomain'
EMAIL_ADDRESS_COMPANY_INFERRED_FROM_EMAIL = 'company_inferred_from_email'
EMAIL_ADDRESS_COUNTRY_INFERRED_FROM_COMPANY = 'country_inferred_from_company'
EMAIL_ALIASE = 'email_aliase'

GITHUB_ISSUES_TIMELINE_TEMPLATE = {
    "search_key": {
        "owner": "",
        "repo": "",
        "number": 0,
        "event": "",
        "updated_at": 0,
        "uuid": "",
        "if_sync": 0
    },
    "deleted": 0,
    "raw_data": {
        "timeline_raw": ""
    }
}

GITHUB_RELEASE_TEMPLATE = {
    "search_key": {
        "owner": "",
        "repo": "",
        "updated_at": 1708934629565
    },
    "raw_data": {
        "html_url": "https://github.com/OWNER/REPO/releases/tag/v1.29.2",
        "id": 142028094,
        "author": {
            "login": "robot",
            "id": 33505452,
            "type": "User"
        },
        "tag_name": "TAG_NAME",
        "target_commitish": "master",
        "name": "RELEASE_NAME",
        "draft": False,
        "prerelease": False,
        "created_at": "2024-02-14T10:32:39Z",
        "published_at": "2024-02-14T18:01:57Z",
        "body": "release desc",
        "reactions": {
            "url": "https://api.github.com/repos/OWNER/REPO/releases/142028094/reactions",
            "total_count": 31,
            "+1": 14,
            "-1": 0,
            "laugh": 2,
            "hooray": 7,
            "confused": 0,
            "heart": 3,
            "rocket": 3,
            "eyes": 2
        }
    }
}
