import Pyro4
import pandas as pd
import time
from multiprocessing import Process, Manager


def make_remote_call(datanode, index, top, genres, return_dict):
    result = datanode.map(top, genres)
    return_dict[index] = result
    return result

class NameNode(object):
    def __init__(self):
        self.datanodes = []

    def find(self, top, genres):
        manager = Manager()
        return_dict = manager.dict()
        jobs = []

        print('Find query: Top {0} Genre {1}'.format(top, genres))
        result_df = pd.DataFrame({
            'source': pd.Series(dtype='str'),
            'original_title': pd.Series(dtype='str'),
            'popularity': pd.Series(dtype='float'),
            'genre_list': pd.Series(dtype='str')})

        start_time = time.time()    
        index = 0    
        for datanode in self.datanodes:
            #results.append(datanode.map(top, genres))
            print('datanode {0}'.format(datanode.name))

            batch=Process(target=make_remote_call,args=(datanode, index, top, genres, return_dict))
            jobs.append(batch)
            batch.start()
            index = index + 1

        for proc in jobs:
            proc.join()

        for i in return_dict:
            print(i)
            result = return_dict[i]
            print('result {0}'.format(len(result)))
            #print(result)            
            for row in result:
                #print(row[0], row[1], row[2])              
                result_df.loc[len(result_df.index)] = [self.datanodes[i].name, row[0], row[1], row[2]]           
            
        end_time = time.time()
        runtime = end_time - start_time
        print("Map time:", round(runtime,10), "sec")
        #result_df = pd.DataFrame(results, columns=["original_title", "popularity", "genre_list"]) 
        start_time = time.time()
        shuffled_df = self.shuffle(result_df)
        end_time = time.time()
        runtime = end_time - start_time
        print("Shuffle time:", round(runtime,10), "sec")

        start_time = time.time()
        reduced_df = self.reduce(shuffled_df, top)
        end_time = time.time()
        runtime = end_time - start_time
        print("Reduce time:", round(runtime,10), "sec")
        return reduced_df

    def shuffle(self, result_df):
        # Sort by multiple columns in a specific order
        shuffled_df = result_df.sort_values(by=['popularity', 'original_title'], ascending=[False, True])
        return shuffled_df

    def reduce(self, shuffled_df, top):
        reduced_df = shuffled_df.nlargest(top, 'popularity')
        return reduced_df

def find_datanodes():
    datanodes = []
    with Pyro4.locateNS() as ns:
        for datanode, datanode_uri in ns.list(prefix="node.mapreduce.").items():
            print("found datanode", datanode)
            datanodes.append(Pyro4.Proxy(datanode_uri))
    if not datanodes:
        raise ValueError("no datanode found! (have you started the data node first?)")
    return datanodes

def main():
    namenode = NameNode()
    namenode.datanodes = find_datanodes()
    
    final_results = namenode.find(10,["Action"])
    print('final_results =>')
    print(final_results)

if __name__ == "__main__":
    main()