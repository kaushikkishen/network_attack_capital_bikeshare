import random
import pandas as pd
import igraph as ig


class CreateGraph:
    def __init__(self, df):
        self.df = df
    
    def create_network(self):
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
        edge_grouped['names'] = edge_grouped['start_station_id'] + '-' \
            + edge_grouped['end_station_id']
        edge_grouped = edge_grouped[['start_station_id', 'end_station_id', 'edge_pairs', 'names']]

        #Create tuplelist from edge_grouped df, read tuplelist in igraph
        g = ig.Graph.TupleList(edge_grouped.itertuples(index=False), \
                            directed=True, edge_attrs=['weight', 'names'])
        return g

class NetworkAttack:
    def __init__(self, graph):
        self.G = graph

    def random_fail(self, f, steps, graph_measures):
        """
        Function: Randomly removes 0-f percentage of nodes in a graph 
        and monitors graph level measures per specified step

        Parameters:
        f = percentage of nodes in graph to be removed
        steps = number of datapoints generated from 0>f
        graph_measures = MUST be a list of iGraph measure names

        Returns: dataframe
        """

        array = []
        sample_count = 0
        node_count = self.G.vcount()
        f_nodecount = round((f*node_count))
        node_delete = random.sample(self.G.vs['name'],f_nodecount)
        sample = int(f_nodecount/steps)
        
        while sample_count/node_count <= f:                            
            if sample < len(node_delete):
                results = []
                to_delete = random.sample(node_delete, sample)
                self.G.delete_vertices(to_delete)
                sample_count += sample
                results.extend([sample_count/node_count, sample_count])
                for measure in graph_measures:
                    method = getattr(ig.Graph, measure)
                    result = method(self.G)
                    results.append(result) 
                array.append(results)
                for node in to_delete:
                    node_delete.remove(node)
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
                for node in to_delete:
                    node_delete.remove(node)
        
        column_names = ['f','f_count']
        measure_count = len(array[1])-2
        measure_names = [f"Measure {i}" for i in range(1, measure_count+1)]
        column_names.extend(measure_names)
        results_df = pd.DataFrame(array, columns =column_names)

        return results_df
