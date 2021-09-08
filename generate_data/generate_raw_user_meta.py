
import math
import random
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

args = [int(x) for x in sys.argv[1:]]

def get_view_counts_to_generate_for_a_user(mean, num_users, enable_plot=False):
    scale = mean * 0.05
    nums = np.random.normal(mean, scale, size=num_users)
    ints = np.absolute(nums).astype(int)
    if enable_plot:
        axis = np.arange(start=min(ints), stop = max(ints) + 1)
        plt.hist(ints, bins = axis)
        
    return ints

def get_random_split_for_buckets(total_count_req):
    low_ratio = random.uniform(0, 0.1)
    low = math.floor(low_ratio * total_count_req)

    medium_ratio = random.uniform(0.3, 0.4)
    medium = math.floor(medium_ratio * total_count_req)

    # high_ratio = 1 - (low_ratio + medium_ratio)
    high = total_count_req - (low + medium)

    return low,medium,high

get_random_split_for_buckets(100)


def generate_user_meta(size, mean):
    df_user = pd.read_csv("data/raw-user-data.csv")
    print ("user data is with us") 
    df_meta = pd.DataFrame()
    df_meta['user_id'] = df_user['user_id']
    df_meta['views_req_for_user'] = get_view_counts_to_generate_for_a_user(mean, size)
    df_meta['views_from_low'],df_meta['views_from_mid'],df_meta['views_from_high'] = zip(*df_meta['views_req_for_user'].map(get_random_split_for_buckets))
    print ("writing to file") 
    df_meta.to_csv('data/user-meta-data.csv')


# mean = 3000
# size = 100000
generate_user_meta(args[0], args[1])
