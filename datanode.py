# saved as nodes.py
import Pyro4
import pandas as pd
import os

@Pyro4.expose
class DataNode(object):
    
    def __init__(self, name, file_name) -> None:
        self.file_name = file_name
        self._name = name
        try:
            self.data_frame = pd.read_csv(self.file_name)
            print('{0} dataframe count {1}'.format(self._name, len(self.data_frame.index)))

        except FileNotFoundError:
            print(f"Error: File '{self.file_name}' not found.")

    @property
    def name(self):
        return self._name
    
    def map(self, top, genres):
        print('{0} request rcvd {1} {2}'.format(self.name, top, genres))
        df = self.data_frame
        
        # Filter the top 3 rows based on 'genres' containing the string 'Action'
        filtered_df = df[df['genre_list'].apply(lambda x: any(item in x for item in genres))]
        print('{0} filtered genre count {1}'.format(self.name, len(filtered_df.index)))

        # Sort the DataFrame by 'popularity' in descending order and take the top 3 rows
        filtered_df = filtered_df.nlargest(top, 'popularity')
        print('{0} filtered top {1} count {2}'.format(self.name, top, len(filtered_df.index)))
        output = []

        for row in filtered_df.itertuples(index=False):
            output.append([row.original_title, row.popularity, row.genre_list])
      
        return output


def get_files_with_prefix(prefix, extension):
    current_directory = os.getcwd()
    file_list = [file_name for file_name in os.listdir(current_directory) if os.path.isfile(os.path.join(current_directory, file_name)) and file_name.startswith(prefix) and file_name.endswith(extension)]
    return file_list

if __name__ == "__main__":
    with Pyro4.Daemon() as daemon:
        dataset_files = get_files_with_prefix("dataset", ".csv")

        for file_name in dataset_files:
            name, extension = os.path.splitext(file_name)
            datanodename= "node.mapreduce." + name
            datanode  = DataNode(datanodename, file_name)
            datanode_uri = daemon.register(datanode)
            with Pyro4.locateNS() as ns:                
                ns.register(datanodename, datanode_uri)
                print(datanodename + " - running")
        print("data nodes available.")
        daemon.requestLoop()




