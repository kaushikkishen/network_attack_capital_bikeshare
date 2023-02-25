import pandas as pd
import networkx as nx

class CreateGraph:
    def __init__(self, df):
        self.df = df
        

    def preprocess_df(self):
        # Convert started_at and ended_at columns to datetime
        self.df['start_time'] = pd.to_datetime(self.df['started_at'])
        self.df['end_time'] = pd.to_datetime(self.df['ended_at'])

        # Extract month and year from start_time and end_time columns
        self.df['start_month'] = self.df['start_time'].dt.month
        self.df['start_year'] = self.df['start_time'].dt.year
        self.df['end_month'] = self.df['end_time'].dt.month
        self.df['end_year'] = self.df['end_time'].dt.year

        # Extract time of day from start_time and end_time columns
        self.df['start_time'] = self.df['start_time'].dt.time
        self.df['end_time'] = self.df['end_time'].dt.time

        # Calculate travel time in seconds
        self.df['travel_time_in_sec'] = (pd.to_datetime(self.df['ended_at'])\
                                          - pd.to_datetime(self.df['started_at'])).dt.total_seconds()

        # Calculate weight column based on the number of trips between each start and end station
        self.df['weight'] = self.df.groupby(['start_station_id', 'end_station_id'])['ride_id']\
            .transform('count')

        # Drop rows with null values
        self.df.dropna(inplace=True)

        return self.df

    def create_network(self):
        G = nx.DiGraph()

        # Add nodes to graph
        for _, row in self.df.iterrows():
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
