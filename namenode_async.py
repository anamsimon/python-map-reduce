import Pyro4
import pandas as pd
import asyncio

class NameNode(object):
    def __init__(self):
        self.datanodes = []

    async def remote_call(self, datanode, top, genres):
        result = await datanode.map(top, genres)
        return result
    
    async def find(self, top, genres):
        
        result_df = pd.DataFrame({
            'source': pd.Series(dtype='str'),
            'original_title': pd.Series(dtype='str'),
            'popularity': pd.Series(dtype='float'),
            'genre_list': pd.Series(dtype='str')})
        
        tasks = []

        for datanode in self.datanodes:
            #print(type(datanode))
            print('datanode {0}'.format(datanode.name))
            print('requesting')
            tasks.append(datanode.map(top, genres))

         # Run all tasks concurrently
        results = await asyncio.gather(*tasks)
          
        for i, result in enumerate(results, start=1):
            print('result {0}'.format(len(result)))
            for row in result:
                result_df.loc[len(result_df.index)] = [datanode.name, row[0], row[1], row[2]]

        shuffled_df = self.shuffle(result_df)
        reduced_df = self.reduce(shuffled_df, top)
        print('final_results =>')
        print(reduced_df)


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
            proxyObj = Pyro4.Proxy(datanode_uri)
            proxyObj._pyroAsync()
            datanodes.append(proxyObj)
    if not datanodes:
        raise ValueError("no datanode found! (have you started the data node first?)")
    return datanodes

def main():
    namenode = NameNode()
    namenode.datanodes = find_datanodes()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(namenode.find(7, ["Mystery"]))
    loop.close()

if __name__ == "__main__":
    main()