import datetime
import json
from collections import defaultdict
from statistics import mean

import networkx
import networkx as nx

from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation
from oss_know.libs.util.log import logger

# Possible social related types:
# merged, reviewed, line-commented, commit-commented
u2u_types = {
    'commented': True,
    'mentioned': True,
    'assigned': True,
    'unassigned': True,
}


def extract_graph_from_ck(ck_client, owner, repo):
    properties_dict = defaultdict(dict)

    user_set = set()
    curtime = datetime.datetime.now()
    sql = f'''
    SELECT search_key__number AS number, search_key__event AS event_type, timeline_raw AS raw
    FROM github_issues_timeline
    WHERE search_key__number GLOBAL IN (SELECT number
                                        FROM github_pull_requests
                                        WHERE search_key__owner = '{owner}'
                                          AND search_key__repo = '{repo}')
      AND search_key__owner = '{owner}'
      AND search_key__repo = '{repo}'
    '''
    timeline_results, user_list = [], []
    timeline_results = ck_client.execute_no_params(sql)
    logger.info(f'execution complete in {datetime.datetime.now() - curtime}')

    sql1 = f'''
    SELECT user__id AS id, user__login AS login, search_key__number AS number, created_at
    FROM github_issues_comments
    WHERE search_key__owner = '{owner}'
      AND search_key__repo = '{repo}'
    '''
    mentioners = defaultdict(str)
    issue_comments = ck_client.execute_no_params(sql1)
    for (user_id, user_login, issue_no, created_at) in issue_comments:
        mentioners[f'{issue_no}_{created_at}'] = (user_id, user_login)

    count = 0
    sql_pr = f'''
    SELECT user__id AS id, user__login as login, number
    FROM github_pull_requests
    WHERE search_key__owner = '{owner}'
      AND search_key__repo = '{repo}'
    '''
    pr_list = ck_client.execute_no_params(sql_pr)
    pr2author = dict()
    for [uid, ulogin, number] in pr_list:
        # pr2author[number] = uid
        pr2author[number] = {
            'id': uid,
            'login': ulogin,
        }
    if not timeline_results:
        return []
    for result in timeline_results:
        if result[1] not in u2u_types:
            continue
        if count % 5000 == 0:
            logger.info(f'{count} records finished in {datetime.datetime.now() - curtime}')
            curtime = datetime.datetime.now()
        count += 1

        pr_num, event_type, timeline = result[0], result[1], json.loads(result[2])
        # TODO Is it better to parse data with dateutil for robustness?
        created_at = timeline['created_at'].replace('T', ' ').replace('Z', '')
        dt_str = created_at[:4] + created_at[5:7] + created_at[8:10]
        if event_type == 'mentioned':
            if not timeline.get('actor'):
                continue
            mentionee_id = timeline['actor']['id']
            mentionee_login = timeline['actor']['login']
            if not mentioners.get(f'{pr_num}_{created_at}'):
                continue
            mentioner, mlogin = mentioners[f'{pr_num}_{created_at}']
            if properties_dict[mentioner].get(mentionee_id) is None:
                properties_dict[mentioner][mentionee_id] = dict()
                properties_dict[mentioner][mentionee_id]['from_developer_id'] = mentioner
                properties_dict[mentioner][mentionee_id]['from_developer_login'] = mlogin
                properties_dict[mentioner][mentionee_id]['to_developer_id'] = mentionee_id
                properties_dict[mentioner][mentionee_id]['to_developer_login'] = mentionee_login
                properties_dict[mentioner][mentionee_id]['dt'] = list()
                properties_dict[mentioner][mentionee_id]['event_types'] = list()
                properties_dict[mentioner][mentionee_id]['weight'] = 0
            if mentioner not in user_set:
                user_set.add(mentioner)
                user_list.append(['u' + str(mentioner), mlogin, mentioner])
            properties_dict[mentioner][mentionee_id]['dt'].append(dt_str)
            properties_dict[mentioner][mentionee_id]['event_types'].append('mentioned')
            properties_dict[mentioner][mentionee_id]['weight'] += 1
        elif event_type == 'commented':
            if not pr2author.get(pr_num):
                continue
            author_id = pr2author[pr_num]['id']
            author_login = pr2author[pr_num]['login']
            commenter_id = timeline['user']['id']
            commenter_login = timeline['user']['login']
            if properties_dict[commenter_id].get(author_id) is None:
                properties_dict[commenter_id][author_id] = dict()
                properties_dict[commenter_id][author_id]['from_developer_id'] = commenter_id
                properties_dict[commenter_id][author_id]['from_developer_login'] = commenter_login
                properties_dict[commenter_id][author_id]['to_developer_id'] = author_id
                properties_dict[commenter_id][author_id]['to_developer_login'] = author_login
                properties_dict[commenter_id][author_id]['dt'] = list()
                properties_dict[commenter_id][author_id]['event_types'] = list()
                properties_dict[commenter_id][author_id]['weight'] = 0
            if commenter_id not in user_set:
                user_set.add(commenter_id)
                user_list.append(['u' + str(commenter_id), commenter_login, commenter_id])
            properties_dict[commenter_id][author_id]['dt'].append(dt_str)
            properties_dict[commenter_id][author_id]['event_types'].append('commented')
            properties_dict[commenter_id][author_id]['weight'] += 1
        elif event_type == 'assigned':
            if timeline.get('assignee') is None:
                continue
            assigner_id = timeline['actor']['id']
            assigner_login = timeline['actor']['login']
            assignee_id = timeline['assignee']['id']
            assignee_login = timeline['assignee']['login']
            if properties_dict[assigner_id].get(assignee_id) is None:
                properties_dict[assigner_id][assignee_id] = dict()
                properties_dict[assigner_id][assignee_id]['from_developer_id'] = assigner_id
                properties_dict[assigner_id][assignee_id]['from_developer_login'] = assigner_login
                properties_dict[assigner_id][assignee_id]['to_developer_id'] = assignee_id
                properties_dict[assigner_id][assignee_id]['to_developer_login'] = assignee_login
                properties_dict[assigner_id][assignee_id]['dt'] = list()
                properties_dict[assigner_id][assignee_id]['event_types'] = list()
                properties_dict[assigner_id][assignee_id]['weight'] = 0
            if assignee_id not in user_set:
                user_set.add(assignee_id)
                user_list.append(['u' + str(assignee_id), assignee_login, assignee_id])
            properties_dict[assigner_id][assignee_id]['dt'].append(dt_str)
            properties_dict[assigner_id][assignee_id]['event_types'].append('assign')
            properties_dict[assigner_id][assignee_id]['weight'] += 1

    # Finally the properties_dict is in the format:
    # {
    # 	"FROM_ID": {
    # 		"TO_ID": {
    # 			"dt": [date1, date2, ...],
    # 			"event_typs": [type1, type2, ...],
    # 			"weight": len(dt_array)
    # 		}
    # 	}
    # }

    # TODO Returning a large list might not be the best option
    #  Currently the POSSIBLY large list is then passed to networkx for graph calculation.
    #  It's better to calculate & save the relations list into a graph db, and then calculate the network metrics.
    relations_list = []
    pr_social_events_batch = []
    pr_social_events_batch_size = 10000
    for from_developer_id, v in properties_dict.items():
        for to_developer_id, p_dict in v.items():
            # for j in range(len(p_dict['dt'])):
            for j, dt in enumerate(p_dict['dt']):
                relations_list.append(['u' + str(from_developer_id), 'u' + str(to_developer_id), j, dt])
                try:
                    pr_social_events_batch.append({
                        'owner': owner,
                        'repo': repo,
                        'from_id': from_developer_id,
                        'from_login': p_dict['from_developer_login'],
                        'to_id': to_developer_id,
                        'to_login': p_dict['to_developer_login'],
                        'event_type': p_dict['event_types'][j],
                        'event_date': datetime.datetime.strptime(dt, '%Y%m%d'),
                        'content': '',
                        'misc': '',
                    })
                except ValueError as e:
                    print(e)
                # TODO This is definitely a nasty approach, need changes here!!!
                if len(pr_social_events_batch) >= pr_social_events_batch_size:
                    ck_client.execute('insert into table pr_social_events values', pr_social_events_batch)
                    pr_social_events_batch = []
    # TODO user_set, user_list are not actually used, remove if not necessary
    #  Store the node-edge-node relations to database
    if pr_social_events_batch:
        ck_client.execute('insert into table pr_social_events values', pr_social_events_batch)
    return relations_list


def extract_metrics(edge_list, owner, repo):
    month_dict = defaultdict(networkx.DiGraph)
    entire_graph = nx.DiGraph()
    month_list = set()
    u2i = {}
    i2u = {}
    node_count = 0
    edge_count = 0
    month_edge_count = defaultdict(int)
    for edge in edge_list:
        src = edge[0]
        dst = edge[1]
        month = str(edge[3])[:-2]
        if month not in month_list:
            month_list.add(month)
        if src not in u2i:
            month_dict[month].add_node(node_count, id=src)
            entire_graph.add_node(node_count, id=src)
            u2i[src] = node_count
            i2u[node_count] = src
            node_count += 1
        if dst not in u2i:
            entire_graph.add_node(node_count, id=dst)
            u2i[dst] = node_count
            i2u[node_count] = dst
        if u2i[src] not in month_dict[month]:
            month_dict[month].add_node(node_count, id=src)
        if u2i[dst] not in month_dict[month]:
            month_dict[month].add_node(node_count, id=dst)

        if month_dict[month].has_edge(u2i[src], u2i[dst]):
            month_dict[month].add_edge(u2i[src], u2i[dst], weight=1 + month_dict[month][u2i[src]][u2i[dst]]['weight'])
        else:
            month_dict[month].add_edge(u2i[src], u2i[dst], weight=1)
        if entire_graph.has_edge(u2i[src], u2i[dst]):
            entire_graph.add_edge(u2i[src], u2i[dst], weight=1 + month_dict[month][u2i[src]][u2i[dst]]['weight'])
        else:
            entire_graph.add_edge(u2i[src], u2i[dst], weight=1)
        month_edge_count[month] += 1
        edge_count += 1
    month_list = sorted(month_list)
    values = []
    for month in month_list:
        g = month_dict[month]
        value = (
            f"{owner}__{repo}",
            month,
            g.number_of_nodes(),
            g.number_of_edges(),
            month_edge_count[month],
            max(nx.out_degree_centrality(g).values()),
            max(nx.in_degree_centrality(g).values()),
            sum(nx.triangles(nx.Graph(g)).values()) / 3,
            nx.transitivity(g),
            mean(nx.clustering(g, weight='weight').values()),
            nx.reciprocity(g),
            nx.density(g),
            nx.number_weakly_connected_components(g),
            mean([d[1] / 2 for d in g.degree(weight='weight')]),
        )
        values.append(value)
    values.append(
        (
            f"{owner}__{repo}",
            "ALL_TIME",
            entire_graph.number_of_nodes(),
            entire_graph.number_of_edges(),
            edge_count,
            max(nx.out_degree_centrality(entire_graph).values()),
            max(nx.in_degree_centrality(entire_graph).values()),
            sum(nx.triangles(nx.Graph(entire_graph)).values()) / 3,
            nx.transitivity(entire_graph),
            mean(nx.clustering(entire_graph, weight='weight').values()),
            nx.reciprocity(entire_graph),
            nx.density(entire_graph),
            nx.number_weakly_connected_components(entire_graph),
            mean([d[1] / 2 for d in entire_graph.degree(weight='weight')]),
        )
    )
    return values


class NetworkMetricRoutineCalculation(MetricRoutineCalculation):
    metric_insert_sql = f'''
    INSERT INTO network_metrics (
    repo, month, num_nodes, num_edges, num_collaborations,
    in_degree_centrality, out_degree_centrality, triangles, transitivity,
    clustering, reciprocity, density, components_number, avg_degree
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

    def calculate_metrics(self):
        logger.info(f'calculating by {self.owner}, {self.repo}')
        edge_list = extract_graph_from_ck(self.clickhouse_client, self.owner, self.repo)
        if not edge_list:
            logger.warning(f'No edges found from {self.owner}/{self.repo} PR events, skip')
            return []

        calculated_metrics = extract_metrics(edge_list, self.owner, self.repo)
        return calculated_metrics

    def save_metrics(self):
        logger.info(f'save metrics with {len(self.batch)} records, to {self.table_name}')
        cursor = self.mysql_conn.cursor()
        cursor.executemany(NetworkMetricRoutineCalculation.metric_insert_sql, self.batch)
        self.mysql_conn.commit()
        logger.info(f"Network metrics of {self.owner}/{self.repo} inserted successfully!")
        cursor.close()

    def routine_calculate_metrics_once(self):
        metrics = self.calculate_metrics()
        self.batch += metrics
        self.save_metrics()
        self.clickhouse_client.close()
        self.mysql_conn.close()
