# library
import numpy as np
import pandas as pd
import multiprocessing
import time
from collections import Counter
import itertools

# data input
train = pd.read_json('drive/My Drive/kakao/data/train.json', typ = 'frame')
val = pd.read_json('drive/My Drive/kakao/data/val.json', typ = 'frame')


# train count
songs = []
tags = []
for i in train.songs:
  songs += i

for i in train.tags:
  tags += i

# val count
val_songs = []
val_tags = []
for i in val.songs:
  val_songs += i

for i in val.tags:
  val_tags += i 

# count unique songs & tags
songs = dict(Counter(songs))
tags =  dict(Counter(tags))

val_songs = dict(Counter(val_songs))
val_tags =  dict(Counter(val_tags))



# sorting by descending order
songs = sorted(songs.items(), key= lambda x: -x[1])
tags = sorted(tags.items(), key= lambda x: -x[1])

val_songs = sorted(val_songs.items(), key= lambda x: -x[1])
val_tags = sorted(val_tags.items(), key= lambda x: -x[1])


# model1 - frequency base
def model(i):
  train['song_inters'] = list(map(lambda x: len(set(val.songs[i]).intersection(set(x))), train.songs))
  train['tag_inters'] = list(map(lambda x: len(set(val.tags[i]).intersection(set(x))), train.tags))
  song_ind = train['song_inters'].sort_values(ascending=False)[:10].index
  tag_ind = train['tag_inters'].sort_values(ascending=False)[:10].index

  song_temp = []
  for i in song_ind:
    song_temp += train.loc[i]['songs']
  
  tag_temp = []
  for i in tag_ind:
    tag_temp += train.loc[i]['tags']

  ans = {}
  ans['id'] = int(val.id[0])
  ans['songs'] = [int(i[0]) for i in Counter(song_temp).most_common(100)]
  ans['tags'] = [i[0] for i in Counter(tag_temp).most_common(100)]
  total.append(ans)


# run
start_time = time.time() 
p_list = range(len(val))
total = []

if __name__ =='__main__':
  pool = multiprocessing.Pool(processes = 2)
  pool.map(model, p_list)
  pool.close()
  pool.join()

json.dumps(total, ensure_ascii=False, indent = '\t')
print('taken time :', start_time - time.time())