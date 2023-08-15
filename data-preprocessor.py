import pandas as pd
import ast
import numpy as np
from IPython.display import display

data = pd.read_csv('movies_metadata.csv')
# adult,belongs_to_collection,budget,genres,
# homepage,id,imdb_id,original_language,
# original_title,overview,popularity,poster_path,
# production_companies,production_countries,
# release_date,revenue,runtime,spoken_languages,
# status,tagline,title,video,vote_average,vote_count

filtered_data = data.filter(['original_title','overview', 'id', 'genres','popularity'], axis=1)

genres = filtered_data['genres'].tolist()
genre_per_movie = []
invalid_genres = ['Aniplex', 'BROSTA TV', 'Carousel Productions', 'GoHands',
                  'Mardock Scramble Production Committee', 'Odyssey Media',
                  'Pulser Productions', 'Rogue State', 'Sentai Filmworks',
                  'Telescene Film Group Productions', 'The Cartel', 'Vision View Entertainment',
                  'TV Movie', 'Foreign']
for genre in genres:
  genre = ast.literal_eval(genre)
  genre_list = []
  for val in genre:
    if val['name'] not in invalid_genres:
      genre_list.append(val['name'])
  genre_per_movie.append(genre_list)

filtered_data['genre_list'] = genre_per_movie
filtered_data = filtered_data[filtered_data['genre_list'].map(lambda d: len(d)) > 0]

filtered_data = filtered_data.filter(['original_title','genre_list','popularity','overview'], axis=1)

filtered_data.to_csv('full_dataset.csv')

df_shuffled = filtered_data.sample(frac=1)
df_splits = np.array_split(df_shuffled, 10)
count = 1
for df in df_splits:
   ##display(df)
   df.to_csv('dataset' + str(count) + '.csv')
   count = count + 1
