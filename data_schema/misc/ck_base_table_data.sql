----------------------------------------------------------------------------------------
-- 更新分布式表 提示：

-- 1/  on cluster <集群名> 必须写,例如: on cluster replicated
---- ALTER TABLE email_domain_to_company_country_local on cluster replicated UPDATE country='US' where country='美国';

-- 2/ 依靠其他列的内容，更新另外一列，where 必须有 如果对整个表 每一行都应用 使用 where 1=1
---- ALTER TABLE github_issues_comments_local on cluster `cluster` UPDATE user__type=splitByString('/users/',user__received_events_url)[2] where 1=1
----------------------------------------------------------------------------------------
-- 删除分布式表&本地表所有数据 提示：
--  truncate table activities_local on cluster replicated
----------------------------------------------------------------------------------------

insert into email_domain_to_company_country
values ('intel.com', 'intel', 'US'),
       ('kernel.intel.com', 'intel', 'US'),
       ('huawei.com', 'huawei', 'CN'),
       ('hisilicon.com', 'huawei', 'CN'),
       ('suse.com', 'suse', 'DE'),
       ('suse.de', 'suse', 'DE'),
       ('suse.fr', 'suse', 'DE'),
       ('redhat.com', 'redhat', 'US'),
       ('google.com', 'google', 'US'),
       ('googlemail.com', 'google', 'US'),
       ('facebook.com', 'facebook', 'US'),
       ('fb.com', 'facebook', 'US'),
       ('arm.com', 'arm', 'UK')