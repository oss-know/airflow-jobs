import json

import networkx as nx

from oss_know.libs.metrics.influence_metrics import MetricRoutineCalculation
from oss_know.libs.util.log import logger


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

        privilege_sql_ = f"""
        WITH {PrivilegeEventsMetricRoutineCalculation.privileged_events_list} as privileged_events
        SELECT search_key__event, timeline_raw FROM github_issues_timeline 
        WHERE search_key__owner = '{self.owner}' AND search_key__repo = '{self.repo}'
        and has(privileged_events, search_key__event)
        """

        # TODO Even with constraints on search_key__event, an iterator is essential when data expands
        #  to a large scale.
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

        for dev, events in privilege_map.items():
            row = [dev]
            row.extend(events)
            response.append(row)

        logger.info(f'{len(response)} Privilege Events Metrics calculated on {self.owner}/{self.repo}')
        return response

    def save_metrics(self):
        logger.info(f'Saving Privilege Events Metrics of {self.owner}/{self.repo}')
        # TODO The string literal can be stored as static class property
        privilege_events_insert_query = '''
            INSERT INTO privilege_events(actor_login, added_to_project, converted_note_to_issue,
                                  deployed, deployment_environment_changed,
                                  locked, merged, moved_columns_in_project,
                                  pinned, removed_from_project,
                                  review_dismissed, transferred,
                                  unlocked, unpinned, user_blocked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)'''
        self.batch_insertion(insert_query=privilege_events_insert_query, batch=self.batch)


class CountMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        # TODO Add and handle author_email
        gits_sql_ = f"""
        SELECT author_name, count() AS commit_count, sum(total__lines) AS total_lines
        FROM gits
        WHERE search_key__owner = '{self.owner}'
          AND search_key__repo = '{self.repo}'
        GROUP BY author_name
        """
        gits_results = self.clickhouse_client.execute_no_params(gits_sql_)

        logger.info(f'Calculating Count Metrics of {self.owner}/{self.repo}')
        return gits_results

    def save_metrics(self):
        count_metrics_insert_query = '''
            INSERT INTO count_metrics(author_name, commit_num, line_of_code)
            VALUES (%s, %s, %s)'''
        self.batch_insertion(insert_query=count_metrics_insert_query, batch=self.batch)
        logger.info(f'{len(self.batch)} Count Metrics saved for {self.owner}/{self.repo}')


class NetworkMetricRoutineCalculation(MetricRoutineCalculation):
    def calculate_metrics(self):
        gits_sql_ = f"""
        SELECT author_name, authored_date, files.file_name FROM gits
        WHERE search_key__owner = '{self.owner}' AND search_key__repo = '{self.repo}'
        """

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
        log = f'Network Metrics of {self.owner}/{self.repo} calculated, {node_num} nodes and ' \
              f'{edge_num} edges in the graph'
        logger.info(log)

        return response

    def save_metrics(self):
        logger.info('saving  Network Metrics')
        network_metrics_insert_query = '''
            INSERT INTO network_metrics(author_name, degree_centrality, eigenvector_centrality)
            VALUES (%s, %d, %f)'''
        self.batch_insertion(insert_query=network_metrics_insert_query, batch=self.batch)
