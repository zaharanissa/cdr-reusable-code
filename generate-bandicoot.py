import os
import csv
import pandas as pd
import bandicoot as bc
import sys
import glob, os
from os import listdir, makedirs
import time
import concurrent.futures

antenna_file = 'antenna.csv'
path_dir = 'user-data-week-25/'
dest_folder = 'bandicoot-week-25/'
start_time = time.time()
files = glob.glob(path_dir + '/*.csv')


def f_proc_result(f):
    indicators = []
    user_id = os.path.basename(f)[:-4]
#     if not os.path.exists('week-9-bandicoot-per-users-non-split-day/'+user_id+'.csv'):
    try:
        B = bc.read_csv(user_id, path_dir, antenna_file, describe=False)
        metrics_dict = bc.utils.all(B, groupby='week', summary='default', network=False, split_week=False, split_day=False,
                                filter_empty=True, attributes=True, flatten=False) 
        home_obj = bc.User.recompute_home(B)
        home_dict = {}
        home_dict['user_home_antenna'] = {}
        home_dict['user_home_antenna']['id'] = getattr(home_obj, 'antenna')
        home_dict['user_home_antenna']['location'] = getattr(home_obj, 'location')
    except Exception as e:
        metrics_dict = {'name': user_id, 'error': str(e)}
    try:
        metrics_dict.update(home_dict)
    except:
        pass
    indicators.append(metrics_dict)
    output_csv = dest_folder+user_id+'.csv'
    bc.to_csv(indicators, output_csv)
    
def run():   
    number = 1
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for f, zipped_file in zip(files, executor.map(f_proc_result, files)):
            print number
            number += 1

if __name__ == "__main__":
        if not os.path.exists(dest_folder): makedirs(dest_folder)
        run()
