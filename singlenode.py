# saved as nodes.py
import Pyro4
import pandas as pd
import os
import time

@Pyro4.expose
class SingleNode(object):
    
    def __init__(self, name, file_name) -> None:
        self.file_name = file_name
        self._name = name
        try:
            self.data_frame = pd.read_csv(self.file_name)
            print('Record count {0}'.format(len(self.data_frame.index)))

        except FileNotFoundError:
            print(f"Error: File '{self.file_name}' not found.")

    @property
    def name(self):
        return self._name
    
    def map(self, top, genres):
        start_time = time.time()
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
        end_time = time.time()
        runtime = end_time - start_time
        print("Map time:", round(runtime,10), "sec")
        return output


def get_files_with_prefix(prefix, extension):
    current_directory = os.getcwd()
    file_list = [file_name for file_name in os.listdir(current_directory) if os.path.isfile(os.path.join(current_directory, file_name)) and file_name.startswith(prefix) and file_name.endswith(extension)]
    return file_list

if __name__ == "__main__":
    with Pyro4.Daemon() as daemon:
        file_name = "full_dataset.csv"
        start_time = time.time()
        name, extension = os.path.splitext(file_name)
        datanodename= "singlenode.mapreduce." + name
        print(datanodename)
        datanode  = SingleNode(datanodename, file_name)
        datanode_uri = daemon.register(datanode)
        with Pyro4.locateNS() as ns:                
            ns.register(datanodename, datanode_uri)    
        end_time = time.time()
        runtime = end_time - start_time
        print("Loadtime:", round(runtime,10), "sec")
        print("Single datanode available.")
        daemon.requestLoop()




