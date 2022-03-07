from oss_know.libs.util.clickhouse_driver import CKServer


def statistics_metrics(clickhouse_server_info):
    ck = CKServer(host=clickhouse_server_info["HOST"],
                  port=clickhouse_server_info["PORT"],
                  user=clickhouse_server_info["USER"],
                  password=clickhouse_server_info["PASSWD"],
                  database=clickhouse_server_info["DATABASE"])
    results = ck.execute_no_params("""
        --1--15
        select 
    	if(a.owner != '',
    	a.owner,
    	b.owner) as owner,
    	if(a.repo != '',
    	a.repo,
    	b.repo) as repo,
    	if(a.github_id != 0,
    	toString(a.github_id),
    	b.github_id) as github_id,
    	if(a.github_id != 0,
    	a.github_login,
    	b.github_login) as github_login,
    	if(a.git_author_email != '',
    	a.git_author_email,
    	'') as git_author_email,
    	commit_times,
    	all_lines,
    	file_counts,
    	user_prs_counts,
    	pr_body_length_avg,
    	user_issues_counts,
    	issue_body_length_avg,
    	issues_comment_count,
    	issues_comment_body_length_avg,
    	pr_comment_count,
    	pr_comment_body_length_avg,
    	be_mentioned_times_in_issues,
    	be_mentioned_times_in_pr,
    	referred_other_issues_or_prs_in_issue,
    	referred_other_issues_or_prs_in_pr,
    	changed_label_times_in_issues,
    	changed_label_times_in_prs,
    	closed_issues_times,
    	closed_prs_times
    from
    	(
    	--1--3
    	--提交了多少次commit，更改了多少行,更改过多少文件 (将对不上账号的剔除掉)
    	select
    		a.search_key__owner as owner,
    		a.search_key__repo as repo,
    		b.author__id as github_id,
    		b.author__login as github_login,
    		author_email as git_author_email,
    		commit_times,
    		all_lines,
    		file_counts
    	from
    		(
    		select
    			commits_all_lines.*,
    			changed_files.file_counts
    		from
    			(
    			select
    				search_key__owner,
    				search_key__repo,
    				author_email,
    				COUNT(author_email) as commit_times,
    				SUM(total__lines) as all_lines
    			from
    				gits
    			group by
    				search_key__owner ,
    				search_key__repo ,
    				author_email
    ) commits_all_lines
    GLOBAL
    		FULL JOIN 
    (
    			select
    				search_key__owner,
    				search_key__repo,
    				author_email,
    				COUNT(*) file_counts
    			from
    				(
    				select
    					DISTINCT `search_key__owner`,
    					`search_key__repo`,
    					`author_email`,
    					`files.file_name` as file_name
    				from
    					`gits`
                   array
    				join `files.file_name`)
    			group by
    				search_key__owner ,
    				search_key__repo ,
    				author_email) changed_files
     ON
    			commits_all_lines.search_key__owner = changed_files.search_key__owner
    			AND 
     commits_all_lines.search_key__repo = changed_files.search_key__repo
    			AND 
     commits_all_lines.author_email = changed_files.author_email ) a
    GLOBAL
    	RIGHT JOIN 
    (
    		select
    			DISTINCT search_key__owner ,
    			search_key__repo ,
    			commit__author__email ,
    			author__id,
    			author__login
    		from
    			github_commits gct
    		where
    			author__id != 0 ) b
    on
    		a.search_key__owner = b.search_key__owner
    		and a.search_key__repo = b.search_key__repo
    		and a.author_email = b.commit__author__email) a 
    GLOBAL
    FULL JOIN 
    (
    	--4 5 6 7 8 9 10 11 12 13 14 15
    	select
    		if(a.owner != '',
    		a.owner,
    		b.owner) as owner,
    		if(a.repo != '',
    		a.repo,
    		b.repo) as repo,
    		if(a.github_id != 0,
    		toString(a.github_id),
    		b.github_id) as github_id,
    		if(a.github_id != 0,
    		a.github_login,
    		b.github_login) as github_login,
    		user_prs_counts,
    		pr_body_length_avg,
    		user_issues_counts,
    		issue_body_length_avg,
    		issues_comment_count,
    		issues_comment_body_length_avg,
    		pr_comment_count,
    		pr_comment_body_length_avg,
    		be_mentioned_times_in_issues,
    		be_mentioned_times_in_pr,
    		referred_other_issues_or_prs_in_issue,
    		referred_other_issues_or_prs_in_pr,
    		changed_label_times_in_issues,
    		changed_label_times_in_prs,
    		closed_issues_times,
    		closed_prs_times
    	from
    		(
    		--指标4，5，6，7
    		select
    			if(c.owner != '',
    			c.owner,
    			d.owner) as owner,
    			if(c.repo != '',
    			c.repo,
    			d.repo) as repo,
    			if(c.github_id != 0,
    			c.github_id,
    			d.github_id) as github_id,
    			if(c.github_id != 0,
    			c.github_login,
    			d.github_login) as github_login,
    			user_prs_counts,
    			pr_body_length_avg,
    			user_issues_counts,
    			issue_body_length_avg,
    			issues_comment_count,
    			issues_comment_body_length_avg,
    			pr_comment_count,
    			pr_comment_body_length_avg
    		from
    			(
    			select
    				if(a.search_key__owner != '',
    				a.search_key__owner,
    				b.search_key__owner) as owner,
    				if(a.search_key__repo != '',
    				a.search_key__repo,
    				b.search_key__repo) as repo,
    				if(a.user__id != 0,
    				a.user__id,
    				b.user__id) as github_id,
    				if(a.user__id != 0,
    				a.user__login,
    				b.user__login) as github_login,
    				a.user_prs_counts,
    				a.body_length_avg as pr_body_length_avg,
    				b.user_issues_counts,
    				b.body_length_avg as issue_body_length_avg
    			from
    				(
    				select
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login,
    					COUNT(user__id) as `user_prs_counts`,
    					avg(lengthUTF8(body)) `body_length_avg`
    				from
    					github_pull_requests
    				group by
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login) a 
    GLOBAL
    			FULL JOIN 
    (
    				select
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login,
    					COUNT(user__id) as `user_issues_counts`,
    					avg(lengthUTF8(body)) `body_length_avg`
    				from
    					github_issues
    				where
    					pull_request__url == ''
    				group by
    					`search_key__owner`,
    					`search_key__repo`,
    					`user__id`,
    					user__login) b
    ON
    				a.user__id = b.user__id
    				and a.search_key__owner = b.search_key__owner
    				and a.search_key__repo = b.search_key__repo) c
    GLOBAL
    		FULL JOIN 
    (
    			select
    				if(a.search_key__owner != '',
    				a.search_key__owner,
    				b.search_key__owner) as owner,
    				if(a.search_key__repo != '',
    				a.search_key__repo,
    				b.search_key__repo) as repo,
    				if(a.user__id != 0,
    				a.user__id,
    				b.user__id) as github_id,
    				if(a.user__id != 0,
    				a.user__login,
    				b.user__login) as github_login,
    				a.issues_comment_count,
    				a.issues_comment_body_length_avg,
    				b.pr_comment_count,
    				b.pr_comment_body_length_avg
    			from
    				(
    				select
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login,
    					COUNT() as issues_comment_count,
    					avg(lengthUTF8(body)) issues_comment_body_length_avg
    				from
    					(
    					select
    						github_issues_comments.*
    					from
    						github_issues_comments global
    					join (
    						select
    							DISTINCT `number`,
    							search_key__owner,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url = '') as pr_number
    on
    						github_issues_comments.search_key__number = pr_number.number
    						and github_issues_comments.search_key__owner = pr_number.search_key__owner
    						and github_issues_comments.search_key__repo = pr_number.search_key__repo )
    				group by
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login) a 
    GLOBAL
    			FULL JOIN 
    (
    				select
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login,
    					COUNT() as pr_comment_count,
    					avg(lengthUTF8(body)) pr_comment_body_length_avg
    				from
    					(
    					select
    						github_issues_comments.*
    					from
    						github_issues_comments global
    					join (
    						select
    							DISTINCT `number`,
    							search_key__owner,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url != '') as pr_number
    on
    						github_issues_comments.search_key__number = pr_number.number
    						and github_issues_comments.search_key__owner = pr_number.search_key__owner
    						and github_issues_comments.search_key__repo = pr_number.search_key__repo )
    				group by
    					search_key__owner,
    					search_key__repo,
    					user__id,
    					user__login) b
    ON
    				a.user__id = b.user__id
    				and a.search_key__owner = b.search_key__owner
    				and a.search_key__repo = b.search_key__repo) d
    ON
    			c.github_id = d.github_id
    			and c.owner = d.owner
    			and c.repo = d.repo) a 
    GLOBAL
    	FULL JOIN 
    (
    		-- 8 9 10 11 12 13 14 15
    		select
    			if(a.owner != '',
    			a.owner,
    			b.owner) as owner,
    			if(a.repo != '',
    			a.repo,
    			b.repo) as repo,
    			if(a.github_id != '',
    			a.github_id,
    			b.github_id) as github_id,
    			if(a.github_id != '',
    			a.github_login,
    			b.github_login) as github_login,
    			be_mentioned_times_in_issues,
    			be_mentioned_times_in_pr,
    			referred_other_issues_or_prs_in_issue,
    			referred_other_issues_or_prs_in_pr,
    			changed_label_times_in_issues,
    			changed_label_times_in_prs,
    			closed_issues_times,
    			closed_prs_times
    		from
    			(
    			--8 9 10 11
    			select
    				if(a.owner != '',
    				a.owner,
    				b.owner) as owner,
    				if(a.repo != '',
    				a.repo,
    				b.repo) as repo,
    				if(a.github_id != '',
    				a.github_id,
    				b.github_id) as github_id,
    				if(a.github_id != '',
    				a.github_login,
    				b.github_login) as github_login,
    				be_mentioned_times_in_issues,
    				be_mentioned_times_in_pr,
    				referred_other_issues_or_prs_in_issue,
    				referred_other_issues_or_prs_in_pr
    			from
    				(
    				--8,9
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.id != '',
    					a.id,
    					b.id) as github_id,
    					if(a.id != '',
    					a.login,
    					b.login) as github_login,
    					be_mentioned_times_in_issues,
    					be_mentioned_times_in_pr
    				from
    					(
    					select
    						search_key__owner,
    						search_key__repo,
    						id,
    						login,
    						COUNT() as be_mentioned_times_in_issues
    					from
    						(
    						select
    							search_key__owner,
    							search_key__repo,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login
    						from
    							(
    							select
    								github_issues_timeline.*
    							from
    								github_issues_timeline global
    							join (
    								select
    									DISTINCT `number`,
    									search_key__owner ,
    									search_key__repo
    								from
    									github_issues gict
    								WHERE
    									pull_request__url = '') as pr_number
    on
    								github_issues_timeline.search_key__number = pr_number.number
    								and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    								and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event')= 'mentioned'
    								and JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1
    )
    					group by
    						search_key__owner,
    						search_key__repo,
    						id,
    						login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						search_key__owner,
    						search_key__repo,
    						id,
    						login,
    						COUNT() as be_mentioned_times_in_pr
    					from
    						(
    						select
    							search_key__owner,
    							search_key__repo,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login
    						from
    							(
    							select
    								github_issues_timeline.*
    							from
    								github_issues_timeline global
    							join (
    								select
    									DISTINCT `number`,
    									search_key__owner ,
    									search_key__repo
    								from
    									github_issues gict
    								WHERE
    									pull_request__url != '') as pr_number
    on
    								github_issues_timeline.search_key__number = pr_number.number
    								and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    								and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event')= 'mentioned'
    								and JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1
    )
    					group by
    						search_key__owner,
    						search_key__repo,
    						id,
    						login) b
    ON
    					a.id = b.id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) a 
    GLOBAL
    			FULL JOIN 
    (
    				--10 11
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.id != '',
    					a.id,
    					b.id) as github_id,
    					if(a.id != '',
    					a.login,
    					b.login) as github_login,
    					referred_other_issues_or_prs_in_issue,
    					referred_other_issues_or_prs_in_pr
    				from
    					(
    					select
    						cross_referenced.search_key__owner,
    						cross_referenced.search_key__repo,
    						id ,
    						login,
    						COUNT(id) as referred_other_issues_or_prs_in_issue
    					from
    						(
    						select
    							search_key__owner ,
    							search_key__repo ,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login,
    							JSONExtractString(JSONExtractString(JSONExtractString(timeline_raw,
    							'source'),
    							'issue'),
    							'number') as number
    						from
    							github_issues_timeline
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event') = 'cross-referenced'
    								AND JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1) cross_referenced
    GLOBAL
    					JOIN 
    (
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url = '') issues
    ON
    						cross_referenced.search_key__owner = issues.search_key__owner
    						and 
    	cross_referenced.search_key__repo = issues.search_key__repo
    						and
    	cross_referenced.number = toString(issues.number)
    					GROUP BY
    						cross_referenced.search_key__owner ,
    						cross_referenced.search_key__repo,
    						id,
    						login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						cross_referenced.search_key__owner,
    						cross_referenced.search_key__repo,
    						id ,
    						login,
    						COUNT(id) as referred_other_issues_or_prs_in_pr
    					from
    						(
    						select
    							search_key__owner ,
    							search_key__repo ,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'id') as id,
    							JSONExtractString(JSONExtractString(timeline_raw,
    							'actor'),
    							'login') as login,
    							JSONExtractString(JSONExtractString(JSONExtractString(timeline_raw,
    							'source'),
    							'issue'),
    							'number') as number
    						from
    							github_issues_timeline
    						WHERE
    							JSONExtractString(timeline_raw,
    							'event') = 'cross-referenced'
    								AND JSONHas(JSONExtractString(timeline_raw,
    								'actor'),
    								'id')= 1) cross_referenced
    GLOBAL
    					JOIN 
    (
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url != '') issues
    ON
    						cross_referenced.search_key__owner = issues.search_key__owner
    						and 
    	cross_referenced.search_key__repo = issues.search_key__repo
    						and
    	cross_referenced.number = toString(issues.number)
    					GROUP BY
    						cross_referenced.search_key__owner ,
    						cross_referenced.search_key__repo,
    						id,
    						login) b
    ON
    					a.id = b.id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) b
    ON
    				a.github_id = b.github_id
    				and a.owner = b.owner
    				and a.repo = b.repo) a 
    GLOBAL
    		FULL JOIN 
    (
    			-- 12 13 14 15
    			select
    				if(a.owner != '',
    				a.owner,
    				b.owner) as owner,
    				if(a.repo != '',
    				a.repo,
    				b.repo) as repo,
    				if(a.github_id != '',
    				a.github_id,
    				b.github_id) as github_id,
    				if(a.github_id != '',
    				a.github_login,
    				b.github_login) as github_login,
    				changed_label_times_in_issues,
    				changed_label_times_in_prs,
    				closed_issues_times,
    				closed_prs_times
    			from
    				(
    				--12 13
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.id != '',
    					a.id,
    					b.id) as github_id,
    					if(a.id != '',
    					a.login,
    					b.login) as github_login,
    					changed_label_times_in_issues,
    					changed_label_times_in_prs
    				from
    					(
    					select
    						github_issues_timeline.search_key__owner ,
    						github_issues_timeline.search_key__repo ,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as login,
    						count(id) as changed_label_times_in_issues
    					from
    						github_issues_timeline  
    global
    					join 
    	(
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url = '') as pr_number
    on
    						github_issues_timeline.search_key__number = pr_number.number
    						and
    	github_issues_timeline.search_key__owner = pr_number.search_key__owner
    						and
    	github_issues_timeline.search_key__repo = pr_number.search_key__repo
    					WHERE
    						(JSONExtractString(timeline_raw,
    						'event')= 'unlabeled'
    							OR 
    	JSONExtractString(timeline_raw,
    							'event')= 'labeled')
    							AND 
    	JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						GROUP BY
    							github_issues_timeline.search_key__owner,
    							github_issues_timeline.search_key__repo,
    							id,
    							login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						github_issues_timeline.search_key__owner ,
    						github_issues_timeline.search_key__repo ,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as login,
    						count(id) as changed_label_times_in_prs
    					from
    						github_issues_timeline  
    global
    					join 
    	(
    						select
    							DISTINCT `number`,
    							search_key__owner ,
    							search_key__repo
    						from
    							github_issues gict
    						WHERE
    							pull_request__url != '') as pr_number
    on
    						github_issues_timeline.search_key__number = pr_number.number
    						and
    	github_issues_timeline.search_key__owner = pr_number.search_key__owner
    						and
    	github_issues_timeline.search_key__repo = pr_number.search_key__repo
    					WHERE
    						(JSONExtractString(timeline_raw,
    						'event')= 'unlabeled'
    							OR 
    	JSONExtractString(timeline_raw,
    							'event')= 'labeled')
    							AND 
    	JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						GROUP BY
    							github_issues_timeline.search_key__owner,
    							github_issues_timeline.search_key__repo,
    							id,
    							login) b
    ON
    					a.id = b.id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) a 
    GLOBAL
    			FULL JOIN 
    (
    				--14 15
    				select
    					if(a.search_key__owner != '',
    					a.search_key__owner,
    					b.search_key__owner) as owner,
    					if(a.search_key__repo != '',
    					a.search_key__repo,
    					b.search_key__repo) as repo,
    					if(a.actor_id != '',
    					a.actor_id,
    					b.actor_id) as github_id,
    					if(a.actor_id != '',
    					a.actor_login,
    					b.actor_login) as github_login,
    					closed_issues_times,
    					closed_prs_times
    				from
    					(
    					select
    						search_key__owner,
    						search_key__repo,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as actor_id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as actor_login,
    						COUNT() closed_issues_times
    					from
    						(
    						select
    							github_issues_timeline.*
    						from
    							github_issues_timeline global
    						join (
    							select
    								DISTINCT `number`,
    								search_key__owner ,
    								search_key__repo
    							from
    								github_issues gict
    							WHERE
    								pull_request__url = '') as pr_number
    on
    							github_issues_timeline.search_key__number = pr_number.number
    							and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    							and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    					WHERE
    						JSONExtractString(timeline_raw,
    						'event')= 'closed'
    							AND JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						group by
    							search_key__owner,
    							search_key__repo,
    							actor_id,
    							actor_login) a 
    GLOBAL
    				FULL JOIN 
    (
    					select
    						search_key__owner,
    						search_key__repo,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'id') as actor_id,
    						JSONExtractString(JSONExtractString(timeline_raw,
    						'actor'),
    						'login') as actor_login,
    						COUNT() closed_prs_times
    					from
    						(
    						select
    							github_issues_timeline.*
    						from
    							github_issues_timeline global
    						join (
    							select
    								DISTINCT `number`,
    								search_key__owner ,
    								search_key__repo
    							from
    								github_issues gict
    							WHERE
    								pull_request__url != '') as pr_number
    on
    							github_issues_timeline.search_key__number = pr_number.number
    							and github_issues_timeline.search_key__owner = pr_number.search_key__owner
    							and github_issues_timeline.search_key__repo = pr_number.search_key__repo)
    					WHERE
    						JSONExtractString(timeline_raw,
    						'event')= 'closed'
    							AND JSONHas(JSONExtractString(timeline_raw,
    							'actor'),
    							'id')= 1
    						group by
    							search_key__owner,
    							search_key__repo,
    							actor_id,
    							actor_login) b
    ON
    					a.actor_id = b.actor_id
    					and a.search_key__owner = b.search_key__owner
    					and a.search_key__repo = b.search_key__repo) b
    ON
    				a.github_id = b.github_id
    				and a.owner = b.owner
    				and a.repo = b.repo) b
    ON
    			a.github_id = b.github_id
    			and a.owner = b.owner
    			and a.repo = b.repo
    ) b
    ON
    		toString(a.github_id)= b.github_id
    			and a.owner = b.owner
    			and a.repo = b.repo) b
    ON
    	toString(a.github_id)= b.github_id
    	and a.owner = b.owner
    	and a.repo = b.repo""")
    all_data = []
    for result in results:
        data = {}
        data['owner'] = result[0]
        data['repo'] = result[1]
        data['github_id'] = int(result[2])
        data['github_login'] = result[3]
        data['git_author_email'] = result[4]
        data['commit_times'] = result[5]
        data['changed_lines'] = result[6]
        data['diff_file_counts'] = result[7]
        data['prs_counts'] = result[8]
        data['pr_body_length_avg'] = result[9]
        data['issues_counts'] = result[10]
        data['issue_body_length_avg'] = result[11]
        data['issues_comment_count'] = result[12]
        data['issues_comment_body_length_avg'] = result[13]
        data['pr_comment_count'] = result[14]
        data['pr_comment_body_length_avg'] = result[15]
        data['be_mentioned_times_in_issues'] = result[16]
        data['be_mentioned_times_in_pr'] = result[17]
        data['referred_other_issues_or_prs_in_issue'] = result[18]
        data['referred_other_issues_or_prs_in_pr'] = result[19]
        data['changed_label_times_in_issues'] = result[20]
        data['changed_label_times_in_prs'] = result[21]
        data['closed_issues_times'] = result[22]
        data['closed_prs_times'] = result[23]
        all_data.append(data)
    ck_sql = "INSERT INTO metrics VALUES"
    ck.execute(ck_sql, all_data)
    return "end::statistics_metrics"



