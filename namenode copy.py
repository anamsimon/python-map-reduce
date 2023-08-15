import Pyro4
import pandas as pd
import asyncio

class NameNode(object):
    def __init__(self):
        self.datanodes = []

    def find(self, top, genres):
        
        result_df = pd.DataFrame({
            'source': pd.Series(dtype='str'),
            'original_title': pd.Series(dtype='str'),
            'popularity': pd.Series(dtype='float'),
            'genre_list': pd.Series(dtype='str')})
        
        tasks = []

        for datanode in self.datanodes:
            #results.append(datanode.map(top, genres))
            print('datanode {0}'.format(datanode.name))
            print('requesting')

            tasks.append(make_remote_call(i))

            result = await async_remote_object.remote_method(value)
            
            result = datanode.map(top, genres)
            print('result {0}'.format(len(result)))
            #print(result)            
            for row in result:
                #print(row[0], row[1], row[2])              
                result_df.loc[len(result_df.index)] = [datanode.name, row[0], row[1], row[2]]
                 
        #result_df = pd.DataFrame(results, columns=["original_title", "popularity", "genre_list"])    
        shuffled_df = self.shuffle(result_df)
        reduced_df = self.reduce(shuffled_df, top)
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
            datanodes.append(Pyro4.asyncproxy(Pyro4.Proxy(datanode_uri)))
    if not datanodes:
        raise ValueError("no datanode found! (have you started the data node first?)")
    return datanodes

async def main():
    namenode = NameNode()
    namenode.datanodes = find_datanodes()
    final_results = namenode.find(7,["Mystery"])
    print('final_results =>')
    print(final_results)
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()