import numpy as np
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
                            directed=True, edge_attrs=['weight', 'name'])
        return g
"""
class NetworkAttack:
    def __init__(self, graph, f):
        self.G = graph
        self.f = f
        self.node_list = list(self.G.nodes)
        self.f_nodecount = round(self.G.number_of_nodes()*f)


    def random_fail(self,step, graph_measures):
        #f is the max percentage of nodes to be deleted
        #create list of numbers from 0 to f
        #steps is the number of interval until f
        f_list = np.linspace(0, self.f, steps).to_list()
        node_delete = np.random.choice(self.node_list,\
                                        size=self.f_nodecount, replace=False)
        
        results = pd.DataFrame(columns=['f','measure'])
        

        if self.f_nodecount%steps==0:
            sample = self.f_nodecount/steps
            for i in f_list:
                to_delete = np.random.choice(node_delete,\
                                             size=sample, replace=False)
                for j in to_delete:
                    self.G.remove_nodes(j)
                    node_delete.remove(j)
                for measure in graph_measures:
                    self.G.measure       
                
        else:

"""