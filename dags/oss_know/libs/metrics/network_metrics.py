import datetime
import json
from collections import defaultdict
from statistics import mean

import networkx
import networkx as nx

from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation


def extract_graph_from_ck(ck_client, owner, repo):
    properties_dict = defaultdict(dict)
    u2u_types = ['commented', 'mentioned', 'assigned', 'unassigned']
    user_set = set()
    sql_time = datetime.timedelta()
    curtime = datetime.datetime.now()
    sql = f"select search_key__number as number, search_key__event as event_type, timeline_raw  as raw from github_issues_timeline where " \
            f"search_key__number global in (select number from github_pull_requests where search_key__owner = '{owner}' and search_key__repo = '{repo}') and " \
    f"search_key__owner = '{owner}' and search_key__repo = '{repo}';"
    timeline_results, user_list = [], []
    timeline_results = ck_client.execute_no_params(sql)
    print(f'execution complete in {datetime.datetime.now() - curtime}')
    count = 0
    sql_pr = f"select user__id as id, number  from github_pull_requests where search_key__owner = '{owner}' and search_key__repo = '{repo}';"
    pr_list = ck_client.execute_no_params(sql_pr)
    pr2author = dict()
    for [uid, number] in pr_list:
        pr2author[number] = uid

    for result in timeline_results:
        if result[1] not in u2u_types:
            continue
        if count % 500 == 0:
            print(f'{count} records finished in {datetime.datetime.now() - curtime}, sql execution {sql_time}')
            curtime = datetime.datetime.now()
        count += 1

        pr_num, event_type, timeline = result[0], result[1], json.loads(result[2])
        created_at = timeline['created_at']
        dt_str = created_at[:4] + created_at[5:7] + created_at[8:10]
        # if event_type == 'mentioned':
        #     mentionee = timeline['actor']['id']
        #     sql1 = f"select user__id as id, user__login as login from github_issues_comments where search_key__number = {pr_num} and created_at = parseDateTimeBestEffort('{created_at}') and  search_key__owner = '{owner}' and search_key__repo = '{repo}';"
        #     df1 = ck_client.execute(sql1)
        #     if not df1:
        #         continue
        #     mentioner = df1[0]
        #     mlogin = df1[1]
        #     if properties_dict[mentioner].get(mentionee) is None:
        #         properties_dict[mentioner][mentionee] = dict()
        #         properties_dict[mentioner][mentionee]['dt'] = list()
        #         properties_dict[mentioner][mentionee]['weight'] = 0
        #         properties_dict[mentioner][mentionee]['owner'] = owner
        #         properties_dict[mentioner][mentionee]['repo'] = repo
        #     if mentioner not in user_set:
        #         user_set.add(mentioner)
        #         user_list.append(['u' + str(mentioner), mlogin, mentioner])
        #         # user_df.append(['u' + str(mentioner), mlogin, mentioner])
        #     properties_dict[mentioner][mentionee]['dt'].append(dt_str)
        #     properties_dict[mentioner][mentionee]['weight'] += 1
        if event_type == 'commented':
            if not pr2author.get(pr_num):
                continue
            author = pr2author[pr_num]
            commenter = timeline['user']['id']
            clogin = timeline['user']['login']
            if properties_dict[commenter].get(author) is None:
                properties_dict[commenter][author] = dict()
                properties_dict[commenter][author]['dt'] = list()
                properties_dict[commenter][author]['repo'] = owner
                properties_dict[commenter][author]['owner'] = repo
                properties_dict[commenter][author]['weight'] = 0
            if commenter not in user_set:
                user_set.add(commenter)
                user_list.append(['u' + str(commenter), clogin, commenter])
            properties_dict[commenter][author]['dt'].append(dt_str)
            properties_dict[commenter][author]['weight'] += 1
        else:
            if timeline.get('assignee') is None:
                continue
            assigner = timeline['actor']['id']
            assignee = timeline['assignee']['id']
            alogin = timeline['assignee']['login']
            if properties_dict[assigner].get(assignee) is None:
                properties_dict[assigner][assignee] = dict()
                properties_dict[assigner][assignee]['dt'] = list()
                properties_dict[assigner][assignee]['owner'] = owner
                properties_dict[assigner][assignee]['repo'] = repo
                properties_dict[assigner][assignee]['weight'] = 0
            if assignee not in user_set:
                user_set.add(assignee)
                user_list.append(['u' + str(assignee), alogin, assignee])
            properties_dict[assigner][assignee]['dt'].append(dt_str)
            properties_dict[assigner][assignee]['weight'] += 1

    relations_list = []
    for a, v in properties_dict.items():
        for b, p_dict in v.items():
            for j in range(len(p_dict['dt'])):
                relations_list.append(
                    ['u' + str(a), 'u' + str(b), j, p_dict['dt'][j]])

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
            sum(nx.triangles(nx.Graph(g)).values())/3,
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
    def calculate_metrics(self):
        print(f'calculating by {self.owner}, {self.repo}')
        edge_list = extract_graph_from_ck(self.clickhouse_client, self.owner, self.repo)
        calculated_metrics = extract_metrics(edge_list, self.owner, self.repo)
        return calculated_metrics

    def save_metrics(self):
        print(f'save metrics with {len(self.batch)} records, to {self.table_name}')
        cursor = self.mysql_conn.cursor()
        insert_query = "INSERT INTO network_metrics (repo, month, num_nodes, num_edges, num_collaborations, in_degree_centrality, out_degree_centrality, triangles, transitivity," \
                       "clustering, reciprocity, density, components_number, avg_degree) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.executemany(insert_query, self.batch)

        self.mysql_conn.commit()
        print("Data inserted successfully!")
        cursor.close()

    def routine_calculate_metrics_once(self):
        metrics = self.calculate_metrics()
        self.batch += metrics
        self.save_metrics()
        self.clickhouse_client.close()
        self.mysql_conn.close()


