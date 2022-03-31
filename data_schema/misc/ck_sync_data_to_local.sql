----统计表--begin--------------------------------------------------------------

truncate table  email_address_local on cluster replicated;
insert into table default.email_address select * from remote (
    '192.168.8.10',
    'default.email_address',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  email_domain_to_company_country_local on cluster replicated;
insert into table default.email_domain_to_company_country select * from remote (
    '192.168.8.10',
    'default.email_domain_to_company_country',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  metrics_local on cluster replicated;
insert into table default.metrics select * from remote (
    '192.168.8.10',
    'default.metrics',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  activities_local on cluster replicated;
insert into table default.activities select * from remote (
    '192.168.8.10',
    'default.activities',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

----统计表--end--------------------------------------------------------------

----基础表--begin--------------------------------------------------------------

truncate table  gits_local on cluster replicated;
insert into table default.gits select * from remote (
    '192.168.8.10',
    'default.gits',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  github_commits_local on cluster replicated;
insert into table default.github_commits select * from remote (
    '192.168.8.10',
    'default.github_commits',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  github_issues_local on cluster replicated;
insert into table default.github_issues select * from remote (
    '192.168.8.10',
    'default.github_issues',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  github_issues_comments_local on cluster replicated;
insert into table default.github_issues_comments select * from remote (
    '192.168.8.10',
    'default.github_issues_comments',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  github_issues_timeline_local on cluster replicated;
insert into table default.github_issues_timeline select * from remote (
    '192.168.8.10',
    'default.github_issues_timeline',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  github_pull_requests_local on cluster replicated;
insert into table default.github_pull_requests select * from remote (
    '192.168.8.10',
    'default.github_pull_requests',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  github_profile_local on cluster replicated;
insert into table default.github_profile select * from remote (
    '192.168.8.10',
    'default.github_profile',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  maillists_local on cluster replicated;
insert into table default.maillists select * from remote (
    '192.168.8.10',
    'default.maillists',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

truncate table  maillists_enriched_local on cluster replicated;
insert into table default.maillists_enriched select * from remote (
    '192.168.8.10',
    'default.maillists_enriched',
    'clickhouse_operator',
    'clickhouse_operator_password') ;

----基础表--end--------------------------------------------------------------