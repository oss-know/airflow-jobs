import json

import networkx as nx
from oss_know.libs.util.log import logger

from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation

class PrivilegeEventsMetricRoutineCalculation(MetricRoutineCalculation):
    privileged_events_list = ["added_to_project", "converted_note_to_issue",
                              "deployed", "deployment_environment_changed",
                              "locked", "merged", "moved_columns_in_project",
                              "pinned", "removed_from_project",
                              "review_dismissed", "transferred",
                              "unlocked", "unpinned", "user_blocked"]

    def calculate_metrics(self):
        event_type_index_map = {}
        for (index, event) in enumerate(self.privileged_events_list):
            event_type_index_map[event] = index

        privilege_sql_ = f"SELECT search_key__event, timeline_raw FROM github_issues_timeline WHERE search_key__owner = '{self.owner}' AND search_key__repo = '{self.repo}'"
        privilege_results = self.clickhouse_client.execute_no_params(privilege_sql_)
        response = []

        privilege_map = {}
        for (event_type, timeline_raw) in privilege_results:
            if event_type in self.privileged_events_list:
                raw_data = json.loads(timeline_raw)
                dev = raw_data['actor']
                dev_name = dev['login']
                if dev_name not in privilege_map:
                    privilege_map[dev_name] = [0] * 14
                index = event_type_index_map[event_type]
                privilege_map[dev_name][index] = 1

        dev_event = []
        for dev, events in privilege_map.items():
            dev_event.append(dev)
            for event in events:
                dev_event.append(event)
            response.append(tuple(dev_event))

        logger.info('calculating  Privilege Events Metrics')
        return response

    def save_metrics(self):
        logger.info('saving  Privilege Events Metrics')
        privilege_events_insert_query = '''
            INSERT INTO privilege_events(actor_login, added_to_project, converted_note_to_issue,
                                  deployed, deployment_environment_changed,
                                  locked, merged, moved_columns_in_project,
                                  pinned, removed_from_project,
                                  review_dismissed, transferred,
                                  unlocked, unpinned, user_blocked)
            VALUES (%s, %d, %d, %d, %d, %d, %d, %d, %d,%d, %d, %d, %d, %d, %d)'''
        self.batch_insertion(insert_query=privilege_events_insert_query, batch=self.batch)


class CountMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        gits_sql_ = f"SELECT author_name, total__lines FROM gits WHERE search_key__owner = '{self.owner}' AND search_key__repo = '{self.repo}'"
        gits_results = self.clickhouse_client.execute_no_params(gits_sql_)
        loc_map = {}
        commit_map = {}
        for (author_name, total_lines) in gits_results:
            if author_name not in loc_map.keys():
                commit_map[author_name] = 1
                loc_map[author_name] = total_lines
            else:
                commit_map[author_name] = commit_map[author_name] + 1
                loc_map[author_name] = loc_map[author_name] + total_lines

        response = []
        for author, commit_num in commit_map.items():
            response.append((author, commit_num, loc_map(author)))

        logger.info('calculating  Count Metrics')
        return response

    def save_metrics(self):
        logger.info('saving  Count Metrics')
        count_metrics_insert_query = '''
            INSERT INTO count_metrics(author_name, commit_num, line_of_code)
            VALUES (%s, %d, %d)'''
        self.batch_insertion(insert_query=count_metrics_insert_query, batch=self.batch)


class NetworkMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        gits_sql_ = f"SELECT author_name, authored_date, files.file_name FROM gits WHERE search_key__owner = '{self.owner}' AND search_key__repo = '{self.repo}'"
        gits_results = self.clickhouse_client.execute_no_params(gits_sql_)
        file_map = {}
        for (author_name, authored_date, file_array) in gits_results:
            year = authored_date.year
            month = authored_date.month
            day = authored_date.day
            day_num = year * 365 + month * 12 + day
            for file in file_array:
                if file not in file_map.keys():
                    file_map[file] = [author_name, day_num]
                else:
                    file_map[file].append([author_name, day_num])

        social_network = nx.Graph()
        commits = list(file_map.values())
        for i in range(len(commits) - 1):
            for j in range(i + 1, len(commits), 1):
                dev1 = commits[i][0]
                dev2 = commits[j][0]
                if dev1 != dev2 and abs(commits[i][1] - commits[j][1]) <= 180:
                    social_network.add_edge(dev1, dev2)

        eigenvector = nx.eigenvector_centrality(social_network)
        response = []
        dev_nodes = list(social_network.nodes())
        for dev in dev_nodes:
            degree = social_network.degree(dev)
            response.append((dev, degree, eigenvector[dev]))

        node_num = social_network.number_of_nodes()
        edge_num = social_network.number_of_edges()

        logger.info(f'calculating  Network Metrics of {self.owner}/{self.repo}, the graph has {node_num} nodes and {edge_num} edges')
        return response

    def save_metrics(self):
        logger.info('saving  Network Metrics')
        network_metrics_insert_query = '''
            INSERT INTO network_metrics(author_name, degree_centrality, eigenvector_centrality)
            VALUES (%s, %d, %f)'''
        self.batch_insertion(insert_query=network_metrics_insert_query, batch=self.batch)
