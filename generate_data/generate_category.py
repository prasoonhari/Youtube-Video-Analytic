import json
import pandas as pd
import numpy as np

# All categories obtained form https://www.kaggle.com/datasnaek/youtube-new?select=US_category_id.json
with open("reference_data/US_category_id.json",'r') as f:
    d = json.load(f)
    
d = d['items']

categories = []

for i in range(len(d)):
    categories.append(d[i]['snippet']['title'])
    
categories = sorted(list(set(categories)))
categories.remove('Shorts') # not sure what this means so I dropped it

codes = [s[:4].upper() for s in categories] # shortened codes for each category that we use latter
print('Total number of categories:', len(categories))
assert(len(set(codes)) == len(categories)) # make sure that codes are unique

rng = np.random.default_rng(42) # random number generator

def targetAge(a=1.5, b=4.5, n=30):
    target = rng.beta(a, b, n) # beta distribution
    # young people have more varieties of interests
    
    # scale to between 18 and 80
    target_age = [int(x*(80-18)+18) for x in target]
    
    return target_age

def truncNormal(mean, sd, n, lo=None, hi=None):
    # normal distribution but within range of lo and hi
    a = rng.normal(mean, sd, n)
    a = [lo if x < lo else hi if x > hi else x for x in a]
    
    return a


age = targetAge()
sd = truncNormal(10, 10, 30, 5, 30)

df = pd.DataFrame({'category':categories, 'cat_code': codes, 'target_age': age, 'sd': sd})

df.to_csv('data/categories.csv',index=False)