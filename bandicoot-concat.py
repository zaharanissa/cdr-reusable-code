import pandas as pd
import hashlib
from os import listdir
import os

root_folder = 'bandicoot-week-21/'
dest_folder = 'bandicoot-result/'

def hashed_user(user_id):
        hashing = hashlib.new('ripemd160')
        b = bytes(user_id)
        hashing.update(b)
        hashed = hashing.hexdigest()
        return hashed

files = os.listdir(root_folder)
collections = []
n = 1
for f in files:
    if f.endswith('.csv'):
        datacsv = pd.read_csv(root_folder + f, header=0, dtype = 'object')
        df = pd.DataFrame(datacsv)
        name_user = df.iloc[0]['name']
        hashed_name = hashed_user(name_user)
        df['name'][0] = hashed_name
        collections.append(df)
        n += 1
        print n

result = pd.concat(collections,ignore_index=True,sort=False)
result.to_csv(dest_folder + 'week-21-result.csv', index=False)