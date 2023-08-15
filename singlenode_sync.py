import Pyro4
import pandas as pd
import time

class NameNode(object):
    def __init__(self):
        self.singlenodes = []

    def find(self, top, genres):
        print('Find query: Top {0} Genre {1}'.format(top, genres))
        result_df = pd.DataFrame({
            'source': pd.Series(dtype='str'),
            'original_title': pd.Series(dtype='str'),
            'popularity': pd.Series(dtype='float'),
            'genre_list': pd.Series(dtype='str')})

        start_time = time.time()        
        for singlenode in self.singlenodes:
            #results.append(singlenode.map(top, genres))
            print('singlenode {0}'.format(singlenode.name))
            result = singlenode.map(top, genres)
            print('result {0}'.format(len(result)))
            #print(result)            
            for row in result:
                #print(row[0], row[1], row[2])              
                result_df.loc[len(result_df.index)] = [singlenode.name, row[0], row[1], row[2]]
        end_time = time.time()
        runtime = end_time - start_time
        print("Map time:", round(runtime,2), "sec")
        #result_df = pd.DataFrame(results, columns=["original_title", "popularity", "genre_list"]) 
        start_time = time.time()
        shuffled_df = self.shuffle(result_df)
        end_time = time.time()
        runtime = end_time - start_time
        print("Shuffle time:", round(runtime,2), "sec")

        start_time = time.time()
        reduced_df = self.reduce(shuffled_df, top)
        end_time = time.time()
        runtime = end_time - start_time
        print("Reduce time:", round(runtime,2), "sec")
        return reduced_df

    def shuffle(self, result_df):
        # Sort by multiple columns in a specific order
        shuffled_df = result_df.sort_values(by=['popularity', 'original_title'], ascending=[False, True])
        return shuffled_df

    def reduce(self, shuffled_df, top):
        reduced_df = shuffled_df.nlargest(top, 'popularity')
        return reduced_df

def find_singlenode():
    singlenodes = []
    with Pyro4.locateNS() as ns:
        for singlenode, singlenode_uri in ns.list(prefix="singlenode.mapreduce.").items():
            print("found singlenode", singlenode)
            singlenodes.append(Pyro4.Proxy(singlenode_uri))
    if not singlenodes:
        raise ValueError("no singlenode found! (have you started the data node first?)")
    return singlenodes

def main():
    namenode = NameNode()
    namenode.singlenodes = find_singlenode()
    
    final_results = namenode.find(10,["Action"])
    print('final_results =>')
    print(final_results)

if __name__ == "__main__":
    main()