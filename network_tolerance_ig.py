import random
import pandas as pd
import igraph as ig
from itertools import zip_longest

""" Tutorial:

1. Place this script in the same folder as your workspace
2. Import this script:

from network_tolerance_ig import *

3. After import, instantiate needed class with the required parameters:

tolerance = GraphTolerance(graph)

4. Using a GraphTolerance function:
measures = ['maxdegree', 'diameter', 'average_path_length']
measure_params = [{'loops': True , 'mode':'all'},
          {'directed':True}]
g1, rf_df_1 = tolerance.random_fail(f=0.05, 
                         graph_measures=measures,
                         measure_params=measure_params)
"""
class CreateGraph:
    def __init__(self):
        pass
    
    def preprocess(self, df):
        df.dropna(inplace=True)
        edge = df[['started_at', 'ended_at',
        'start_station_id', 'start_station_name',
       'end_station_id', 'end_station_name', 'member_casual']].copy()

        edge['start_time'] = pd.to_datetime(edge['started_at'])
        edge['end_time'] = pd.to_datetime(edge['ended_at'])

        # Extract month and year from start_time and end_time columns
        edge['start_year'] = edge['start_time'].dt.year
        edge['end_year'] = edge['end_time'].dt.year

        # Calculate travel time in seconds
        edge['travel_time_in_sec'] = (pd.to_datetime(edge['ended_at'])\
                                          - pd.to_datetime(edge['started_at'])).dt.total_seconds()
        
        edge.drop(columns=['start_time', 'end_time'], inplace=True)

        #Converts station id numbers to string
        edge['start_station_id'] = edge['start_station_id'].astype(int).astype('string')
        edge['end_station_id'] = edge['end_station_id'].astype(int).astype('string')

        #concat START and END ids to create edge pair ID column
        edge['edge_pairs'] = edge['start_station_id'] + '-' + edge['end_station_id']
        #clean up member_casual columns
        edge['member_casual'] = edge['member_casual'].str.lower()
        edge = pd.get_dummies(edge, columns =['member_casual'])


        #Groupby edge_pairs with count as method
        edge_grouped = edge.groupby(['edge_pairs'], as_index=False)\
            .agg({'start_station_id':'first',
                'end_station_id':'first',
                'edge_pairs': 'count' ,
                'started_at': 'first',
                'ended_at':'first',
                'start_station_name':'first',	
                'end_station_name':'first',
                'member_casual_casual': 'sum',
                'member_casual_member': 'sum',	
                'start_year': 'first',	
                'end_year': 'first',
                'travel_time_in_sec': 'sum'})


        #Concat start and end ID column again to be used as edge name
        #Rearrange columns for igraph ingest
        edge_grouped['name'] = edge_grouped['start_station_id'] + '-' \
            + edge_grouped['end_station_id']
        edge_grouped.rename(columns={'edge_pairs':'weight',
                                    'member_casual_casual': 'casual_count',
                                    'member_casual_member': 'member_count'
                                    }, inplace=True)

        edge_grouped = edge_grouped[['start_station_id', 'end_station_id', 
                                    'weight', 'name',
                                    'started_at',
                                    'ended_at',
                                    'start_station_name',	
                                    'end_station_name',
                                    'casual_count',
                                    'member_count',	
                                    'start_year',
                                    'end_year',
                                    'travel_time_in_sec']]


        #Create tuplelist from edge_grouped df, read tuplelist in igraph

        return edge_grouped

    def create_network(self, df, directed=False):
        g = ig.Graph.DataFrame(df, directed=directed, use_vids=False)
        return g
    
class GraphTolerance:
    """Includes functions for error and attack tolerance"""
    
    def __init__(self, graph):
        self.G = graph.copy()
    
    def measure_calc(self, g, graph_measures, kwargs={}):
        measures = []
        for measure, kwarg in zip_longest(graph_measures, kwargs, fillvalue={}):
                method = getattr(ig.Graph, measure)
                result = method(g, **kwarg)
                measures.append(result)
        return measures

    def random_fail(self, f=0.05, graph_measures=['diameter'],\
                     measure_params={}):
        
        """Error Tolerance Method

        Function: Randomly removes f percentage of nodes in a graph
        and ouputs a dataframe on graph-level measures after removal

        Parameters:
        f = percentage of nodes in graph to be removed
        graph_measures = MUST be a list or tuple of graph level methods
        measure_params = dictionary or list of dictionaries of graph_measures
        parameters

        Returns: graph, dataframe
        """
        g = self.G.copy()
      
        array = []
        sample_count = 0
        node_count = g.vcount()
        f_nodecount = round((f*node_count))
        node_delete = random.sample(g.vs['name'],f_nodecount)
        
        if type(graph_measures) != list and type(graph_measures) != tuple:
           raise ValueError("Measures must be a string list or tuple")
        
        while sample_count != f_nodecount:                            
            results = []
            g.delete_vertices(node_delete[0])
            sample_count += 1
            results.extend([sample_count/node_count, sample_count])
            results.extend(self.measure_calc(g, graph_measures, measure_params))
            array.append(results)
            node_delete.remove(node_delete[0])
        
        column_names = ['f','f_count']
        column_names.extend(graph_measures)
        results_df = pd.DataFrame(array, columns = column_names)

        return g, results_df

    def target_attack(self, f=0.05, centrality='degree', centrality_params={}, \
                      graph_measures=['diameter'], measure_params={}):
        
        """Targeted Attack Method

        Function: Removes top-f percent of nodes with highest centrality measure
        and ouputs a dataframe on graph-level measures after removal

        Parameters:
        f = max percentage of nodes in graph to be removed
        centrality = node centrality measure to do targeted attack
        centrality_params = dictionary or list of dictionaries of centrality
        parameters
        graph_measures = MUST be a list or tuple of graph level methods
        measure_params = dictionary or list of dictionaries of graph_measures
        parameters

        Returns: graph, dataframe
        """
        g = self.G.copy()

        array = []
        sample_count = 0
        node_count = g.vcount()
        f_nodecount = round((f*node_count))

        if type(graph_measures) != list and type(graph_measures) != tuple:
            raise ValueError("Measures must be a string list or tuple")

        while sample_count != f_nodecount:                       
            results = []
            bench = getattr(ig.Graph, centrality)
            bench_compute = bench(g, **centrality_params)
            bench_index = bench_compute.index(max(bench_compute))
            g.delete_vertices(bench_index)
            sample_count += 1
            results.extend([sample_count/node_count, sample_count])
            results.extend(self.measure_calc(g, graph_measures, measure_params))
            array.append(results)

        column_names = ['f','f_count']
        column_names.extend(graph_measures)
        results_df = pd.DataFrame(array, columns = column_names)

        return g, results_df