import random
import pandas as pd
import igraph as ig
import numpy as np

class CreateGraph:
    def __init__(self, df):
        self.df = df
    
    def measure_calc(graph_measures):
        loop_calc = ['maxdegree', 'density']
        measures = []
        for measure in graph_measures:
                if measure not in loop_calc:
                    method = getattr(ig.Graph, measure)
                    result = method(G)
                    measures.append(result)
                else:
                    method = getattr(ig.Graph, measure)
                    result = method(G, loops=True)
                    measures.append(result)
        return measures
    
    def create_network(self, directed, loops):
        edge = self.df[['start_station_id', 'end_station_id']].copy()

        edge.dropna(inplace=True)

        #Converts station id numbers to string
        edge['start_station_id'] = edge['start_station_id'].astype(int).astype('string')
        edge['end_station_id'] = edge['end_station_id'].astype(int).astype('string')
    
        #concat START and END ids to create edge pair ID column
        edge['edge_pairs'] = edge['start_station_id'] + '-' + edge['end_station_id']

        #Groupby edge_pairs with count as method
        edge_grouped = edge.groupby(['edge_pairs'], as_index=False)\
            .agg({'edge_pairs': 'count', 'start_station_id':'first', 'end_station_id':'first'})

        #Concat start and end ID column again to be used as edge name
        #Rearrange columns for igraph ingest
        edge_grouped['name'] = edge_grouped['start_station_id'] + '-' \
            + edge_grouped['end_station_id']
        edge_grouped.rename(columns={'edge_pairs':'weight'}, inplace=True)
        edge_grouped = edge_grouped[['start_station_id', 'end_station_id', 'weight', 'name']]
        #Create tuplelist from edge_grouped df, read tuplelist in igraph
        g = ig.Graph.DataFrame(edge_grouped, \
                    directed=True, use_vids=False)
        return g

class GraphTolerance:
    """Includes functions for error and attack tolerance"""
    def __init__(self, graph):
        self.G = graph

    def random_fail(self, f, steps, graph_measures):
        """Error Tolerance Method

        Function: Randomly removes 0-f percentage of nodes in a graph 
        and monitors graph level measures per step

        Parameters:
        f = max percentage of nodes in graph to be removed
        steps = minimum number of datapoints generated from 0 to f
        graph_measures = MUST be a list or tuple of graph level methods

        Returns: dataframe
        """
      

        array = []
        sample_count = 0
        node_count = self.G.vcount()
        f_nodecount = round((f*node_count))
        node_delete = random.sample(self.G.vs['name'],f_nodecount)
        sample = int(f_nodecount/steps)
        
        if sample <1:
            raise ValueError("Steps greater than nodes to be removed")
        
        if type(graph_measures) != list and type(graph_measures) != tuple:
            raise ValueError("Measures must be a string list or tuple")
        
        while sample_count != f_nodecount:                            
            if sample < len(node_delete):
                results = []
                to_delete = node_delete[:sample]
                self.G.delete_vertices(to_delete)
                sample_count += sample
                results.extend([sample_count/node_count, sample_count])
                for measure in graph_measures:
                    method = getattr(ig.Graph, measure)
                    result = method(self.G)
                    results.append(result) 
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
                    self.G.delete_vertices(to_delete)
                    sample_count += len(to_delete)
                    results.extend([sample_count/node_count, sample_count])
                    for measure in graph_measures:
                        method = getattr(ig.Graph, measure)
                        result = method(self.G)
                        results.append(result) 
                    array.append(results)
                    if len(node_delete) == 0:
                        break
                    else:
                        for node in to_delete:
                            node_delete.remove(node)
        
        column_names = ['f','f_count']
        column_names.extend(graph_measures)
        results_df = pd.DataFrame(array, columns = column_names)

        return results_df

    def target_attack(self, f, steps, graph_measures):
        """Targeted Attack Method

        Function: Randomly removes 0-f percentage of nodes in a graph 
        and monitors graph level measures per step

        Parameters:
        f = max percentage of nodes in graph to be removed
        steps = minimum number of datapoints generated from 0 to f
        graph_measures = MUST be a list or tuple of graph level methods

        Returns: dataframe
        """

        array = []
        sample_count = 0
        node_count = self.G.vcount()
        f_nodecount = round((f*node_count))
        degrees = np.array(self.G.degree())
        sorted_indices = np.argsort(-degrees)
        node_delete = self.G.vs[sorted_indices[:f_nodecount]]
        node_delete['name']
        sample = int(f_nodecount/steps)
        
        if sample <1:
            raise ValueError("Steps greater than nodes to be removed")
        
        if type(graph_measures) != list and type(graph_measures) != tuple:
            raise ValueError("Measures must be a string list or tuple")
        
        while sample_count != f_nodecount:                            
            if sample < len(node_delete):
                results = []
                to_delete = node_delete[:sample]
                self.G.delete_vertices(to_delete)
                sample_count += sample
                results.extend([sample_count/node_count, sample_count])
                for measure in graph_measures:
                    method = getattr(ig.Graph, measure)
                    result = method(self.G)
                    results.append(result) 
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
                    self.G.delete_vertices(to_delete)
                    sample_count += len(to_delete)
                    results.extend([sample_count/node_count, sample_count])
                    for measure in graph_measures:
                        method = getattr(ig.Graph, measure)
                        result = method(self.G)
                        results.append(result) 
                    array.append(results)
                    if len(node_delete) == 0:
                        break
                    else:
                        for node in to_delete:
                            node_delete.remove(node)
        
        column_names = ['f','f_count']
        column_names.extend(graph_measures)
        results_df = pd.DataFrame(array, columns = column_names)

        return results_df
