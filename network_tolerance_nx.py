import numpy as np
import random
import pandas as pd
import networkx as nx

class CreateGraph:
    def __init__(self):
        pass
        
    def preprocess_df(self, df):
        # Drop rows with null values
        df.dropna(inplace=True)
        # Convert started_at and ended_at columns to datetime
        df['start_time'] = pd.to_datetime(df['started_at'])
        df['end_time'] = pd.to_datetime(df['ended_at'])

        # Extract month and year from start_time and end_time columns
        df['start_month'] = df['start_time'].dt.month
        df['start_year'] = df['start_time'].dt.year
        df['end_month'] = df['end_time'].dt.month
        df['end_year'] = df['end_time'].dt.year

        # Extract time of day from start_time and end_time columns
        df['start_time'] = df['start_time'].dt.time
        df['end_time'] = df['end_time'].dt.time

        # Calculate travel time in seconds
        df['travel_time_in_sec'] = (pd.to_datetime(df['ended_at'])\
                                          - pd.to_datetime(df['started_at'])).dt.total_seconds()
       
        # Calculate weight column based on the number of trips between each start and end station
        df['weight'] = df.groupby(['start_station_id', 'end_station_id'])['ride_id'].transform('count')

        return df

    def create_network(self, df):
        G = nx.DiGraph()

        # Add nodes to graph
        for _, row in df.iterrows():
            # Add start station node
            start_id = row['start_station_id']
            start_name = row['start_station_name']
            start_lat = row['start_lat']
            start_lng = row['start_lng']
            if not G.has_node(start_id):
                G.add_node(start_id, name=start_name, lat=start_lat, lng=start_lng)

            # Add end station node
            end_id = row['end_station_id']
            end_name = row['end_station_name']
            end_lat = row['end_lat']
            end_lng = row['end_lng']
            if not G.has_node(end_id):
                G.add_node(end_id, name=end_name, lat=end_lat, lng=end_lng)

            # Add ride edge
            ride_id = row['ride_id']
            rideable_type = row['rideable_type']
            member_casual = row['member_casual']
            start_time = row['started_at']
            end_time = row['ended_at']
            travel_time_in_sec = row['travel_time_in_sec']
            weight = row['weight']
            G.add_edge(start_id, end_id, 
                    id=ride_id, 
                    type=rideable_type, 
                    member_casual=member_casual, 
                    start_time=start_time, 
                    end_time=end_time, 
                    travel_time_in_sec=travel_time_in_sec, 
                    weight=weight)

        return G

class GraphTolerance:
    """Includes functions for error and attack tolerance"""
    def __init__(self, graph):
        self.G = graph
    
    def measure_calc(self, graph_measures, custom_measures=None):
        measures = []
        for measure in graph_measures:
                method = getattr(nx, measure)
                result = method(self.G)
                measures.append(result)
        
        if custom_measures:
            for measure in custom_measures:
                custom_func = custom_measures.get(measure)
                custom_result = custom_func(self.G)
                measures.append(custom_result)
                    
        return measures

    def random_fail(self, f, steps, graph_measures, custom_measures):
        """Error Tolerance Method

        Function: Randomly removes f percentage of nodes in a graph
        and ouputs a dataframe on graph-level measures after removal

        Parameters:
        f = percentage of nodes in graph to be removed
        steps = minimum number of datapoints generated from 0 to f
        graph_measures = MUST be a list or tuple of graph level methods
        custom_measures = Dictionary of customfunctions that calculate graph-level
        attriutes using NetworkX. Mostly for error-handling NetworkX methods
        or if there is no built-in method

        Note: High f values might lead to errors due to lack of error-handling
        of NetworkX methods

        Returns: dataframe
        """
      
        array = []
        sample_count = 0
        node_count = self.G.number_of_nodes()
        f_nodecount = round((f*node_count))
        node_delete = random.sample(list(self.G.nodes()),f_nodecount)
        sample = int(f_nodecount/steps)

        if sample <1:
            raise ValueError("Steps greater than nodes to be removed")

        if type(graph_measures) != list and type(graph_measures) != tuple:
            raise ValueError("Measures must be a string list or tuple")

        while sample_count != f_nodecount:                            
            if sample < len(node_delete):
                results = []
                to_delete = node_delete[:sample]
                self.G.remove_nodes_from(to_delete)
                sample_count += sample
                results.extend([sample_count/node_count, sample_count])
                results.extend(self.measure_calc(graph_measures, custom_measures))
                array.append(results)
                if len(node_delete) == 0:
                    break
                else:
                    for node in to_delete:
                        node_delete.remove(node)
            else:
                if len(node_delete) == 0:
                    break
                else:
                    results = []
                    to_delete = node_delete
                    self.G.remove_nodes_from(to_delete)
                    sample_count += len(to_delete)
                    results.extend([sample_count/node_count, sample_count])
                    results.extend(self.measure_calc(graph_measures, custom_measures))
                    array.append(results)
                    if len(node_delete) == 0:
                        break
                    else:
                        for node in to_delete:
                            node_delete.remove(node)

        column_names = ['f','f_count']
        column_names.extend(graph_measures)
        column_names.extend(list(custom_measures.keys()))
        results_df = pd.DataFrame(array, columns = column_names)

        return results_df

    def target_attack(self, f, steps, graph_measures, custom_measures):
        """Targeted Attack Method

        Function: Removes top-f percent of nodes with highest degree
        and ouputs a dataframe on graph-level measures after removal

        Parameters:
        f = max percentage of nodes in graph to be removed
        steps = minimum number of datapoints generated from 0 to f
        graph_measures = MUST be a list or tuple of graph level methods
        custom_measures = Dictionary of custom functions that calculate graph-level
        attriutes using NetworkX. Mostly for error-handling NetworkX methods
        or if there is no built-in method.

        Note: High f values might lead to errors due to lack of error-handling
        of NetworkX methods

        Returns: dataframe
        """

        array = []
        sample_count = 0
        node_count = self.G.number_of_nodes()
        f_nodecount = round((f*node_count))
        degrees = np.array([(n, d) for n, d in self.G.degree()])
        sorted_indices = np.argsort(-degrees[:, 1])
        top_vertices = degrees[sorted_indices[:f_nodecount], 0]
        node_delete = list(top_vertices)
        sample = int(f_nodecount/steps)
        
        if sample <1:
            raise ValueError("Steps greater than nodes to be removed")

        if type(graph_measures) != list and type(graph_measures) != tuple:
            raise ValueError("Measures must be a string list or tuple")

        while sample_count != f_nodecount:                            
            if sample < len(node_delete):
                results = []
                to_delete = node_delete[:sample]
                self.G.remove_nodes_from(to_delete)
                sample_count += sample
                results.extend([sample_count/node_count, sample_count])
                results.extend(self.measure_calc(graph_measures, custom_measures))
                array.append(results)
                if len(node_delete) == 0:
                    break
                else:
                    for node in to_delete:
                        node_delete.remove(node)
            else:
                if len(node_delete) == 0:
                    break
                else:
                    results = []
                    to_delete = node_delete
                    self.G.remove_nodes_from(to_delete)
                    sample_count += len(to_delete)
                    results.extend([sample_count/node_count, sample_count])
                    results.extend(self.measure_calc(graph_measures, custom_measures))
                    array.append(results)
                    if len(node_delete) == 0:
                        break
                    else:
                        for node in to_delete:
                            node_delete.remove(node)

        column_names = ['f','f_count']
        column_names.extend(graph_measures)
        column_names.extend(list(custom_measures.keys()))
        results_df = pd.DataFrame(array, columns = column_names)

        return results_df