import json
import math
import random
import string
import sys
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rstr
import scipy.special as sps
from faker import Faker
from faker.providers.internet.en_US import Provider as urlProvider
from faker.providers.job.en_US import Provider as jobProvider
from faker.providers.person.en import Provider as personProvider
from geopy.geocoders import Nominatim

# num_users_to_generate = 100

# List of countries- just 10 countries for now.
# countries = ["Australia","Brazil","Canada","China","France","Korea","India","Mexico","US","UK"]
countries = ["Canada","US","UK"]
jobs = ["Accountant","Actor","Architect","Author","Cleaner","Chef","Designer","Doctor","Electrician","Engineer","Farmer","Journalist","Lawyer","Lecturer","Mechanic","Nurse","Pilot","Politician","Policeman","Scientist","Soldier","Teacher","Real estate agent","Travel agent","College Student","Factory worker","Photographer","School Student","Youtuber","Freelancer","Social Media influencer","Gamer","Model","Private Business","Govenment Worker"]

fake = Faker()

country_to_coordinates = {}
geolocator = Nominatim(user_agent="tars")
for country in countries :
    country_to_coordinates[country] = (geolocator.geocode(country).latitude, geolocator.geocode(country).longitude)

# Creating userId - startU defines the starting index of the sequence
# of Ids
def unique_user_id(start_u,size):
    userIds = np.arange(start_u,start_u+size).astype(str)
    return userIds

# Location biases of a user - scale of 0 to 1 - 1 being extremely local
def user_loc_bias(size):
    user_loc_bias_clean = pd.to_numeric(np.absolute(np.random.normal(loc=.3, scale=.1, size=size)))
    return user_loc_bias_clean
# User video toleration level defined in minutes.
def user_tolerance_level(size): 
    # 1 means its extremely tolerant to length off video.
    return np.absolute(np.random.power(5, size))

def user_Resolution_preference(size, p=None):
    if not p:
        # default probabilities
        p = (0.10, 0.70, 0.20)
    resolution_preference = ("HD","ED","SD")
    return np.random.choice(resolution_preference, size=size, p=p)  

#Random jobs - using all the jobs provided by faker
def random_job(size):  
    # jobs = getattr(jobProvider, 'jobs')
    # return np.random.choice(jobs, size=size)
    return np.random.choice(jobs, size=size)

def random_names(name_type, size):  
    names = getattr(personProvider, name_type)
    return np.random.choice(names, size=size)

def random_age(size):
    # shape, scale = 30., 15.
    # s = np.random.gamma(shape, scale, 1000)
    # count, bins, ignored = plt.hist(s, 50, density=True)
    # y = bins**(shape-1)*(np.exp(-bins/scale) /  
    #                  (sps.gamma(shape)*scale**shape))
    # plt.plot(bins, y, linewidth=2, color='r')  
    # plt.show()
    return pd.to_numeric(np.absolute(np.random.gamma(15.,2.,size))).astype(int)

# Random gender - with some noise 
def random_genders(size, p=None):
    if not p:
        # default probabilities
        p = (0.40, 0.40, 0.01, 0.06,0.02,0.07,0.03,0.002,0.002,0.002,0.002,0.002)
        # p = (0.40, 0.40, 0.01, 0.06,0.02,0.07,0.04)
    gender = ("M", "F", "O", "MALE","Female","f","FEMALE","ED","MF","Fimale","Malee","FT")
    # gender = ("M", "F", "O", "MALE","Female","f","FEMALE")
    return np.random.choice(gender, size=size, p=p)

# Random date generation between a particular start and end date
def random_dates(start, end, size):
    start_u = start.value//10**9
    end_u = end.value//10**9
    return pd.DatetimeIndex((10**9*np.random.randint(start_u, end_u, size, dtype=np.int64)).view('M8[ns]'))

def random_locations(size, p=None):
    if not p:
        #  p = (0.08,0.05,0.07,0.05,0.04,0.1,0.15,0.1,0.2,0.16)
        p = (0.3,0.5,0.2)
        return np.random.choice(countries, size=size, p = p)

# Category interest- Defined as per the categories.csv 
# Approximately 95% of the data is within two standard deviations 
# (higher or lower) from the mean.
def category_interest(x,category):
    category_filter = category.loc[(category['target_age'] + 2*category['sd'] > x)& (category['target_age'] - 2*category['sd'] < x)]
    if category_filter.empty:
        return np.random.choice(category["category"])
    else:
        
        return np.random.choice(category_filter["category"])


col_list = ["category","target_age","sd"]
category = pd.read_csv("data/categories.csv",usecols=col_list)

def get_view_counts_to_generate_for_a_user(num_users, enable_plot=False):
    nums = np.random.normal(5000, scale=2000, size=num_users)
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

# view_count_distribution = get_view_counts_to_generate_for_a_user(num_users_to_generate)

def generate_user_data(size):
    df = pd.DataFrame()
    df_meta = pd.DataFrame()
    user_id_starting_index = 300000
    df['join_date'] = random_dates(start=pd.to_datetime('2004-01-01'), end=pd.to_datetime('2019-12-31'), size=size)
    df['first'] = random_names('first_names', size) 
    df['last'] = random_names('last_names', size) 
    df['age'] = random_age(size)
    df['gender'] = random_genders(size)
    df['occupation'] = random_job(size) 
    df['location'] = random_locations(size)
    df['lat'] = df['location'].apply(lambda x: country_to_coordinates[x][0]) 
    df['long'] = df['location'].apply(lambda x: country_to_coordinates[x][1])
    df['user_id'] = unique_user_id(user_id_starting_index,size) 
    df['location_bias'] = user_loc_bias(size) 
    df['toleration_level'] = user_tolerance_level(size)
    df['resolution_preference'] = user_Resolution_preference(size)  
    df['interest'] = df['age'].apply(lambda x: category_interest(x,category))
    df['url'] = df['user_id'].apply(lambda x: 'http://youtube.com/user={0}'.format(x))

    # Noise added based on selected field
    df['last']= df['last'].mask(np.random.random(df['last'].shape) <.007)
    df['first']= df['first'].mask(np.random.random(df['first'].shape) <.007)
    df['occupation']= df['occupation'].mask(np.random.random(df['occupation'].shape) <.007)
    df['join_date']=df['join_date'].mask(np.random.random(df['join_date'].shape) <.007,df['join_date'].apply(lambda x: fake.date_between_dates(date_start=datetime(2015,1,1), date_end=datetime(2019,12,31))))
    df['location']=df['location'].mask(np.random.random(df['location'].shape) <.007,df['location'].apply(lambda x: ''.join(random.choices(string.ascii_uppercase + string.digits, k=np.random.randint(low=0,high=20)))))
    df['url'] = df['url'].mask(np.random.random(df['url'].shape) < .007,df['url'].apply(lambda x: 'http://' +''.join(random.choices(string.ascii_letters,k=np.random.randint(low=0,high=20)))+'.com/user={0}'))
    df['age'] = df['age'].mask(df['age']==np.random.randint(low=0,high=100),-df['age']*np.random.randint(low=1,high=3000))
    df['location_bias'] = df['location_bias'].mask(np.random.random(df['toleration_level'].shape)<.007,-df['location_bias']*np.random.randint(low=1,high=200))
    df['toleration_level'] = df['toleration_level'].mask(np.random.random(df['toleration_level'].shape) < .007,-df['toleration_level']*np.random.randint(low=1000,high=2000))

    # df_meta['user_id'] = df['user_id']
    df_meta['user_id'] = df['user_id']
    df_meta['views_req_for_user'] = get_view_counts_to_generate_for_a_user(size)
    df_meta['views_from_low'],df_meta['views_from_mid'],df_meta['views_from_high'] = zip(*df_meta['views_req_for_user'].map(get_random_split_for_buckets))


    # df_meta.to_csv('data/raw-user-meta.csv')
    df.to_csv('data/raw-user-data.csv')

generate_user_data(int(sys.argv[1]))

# generate_user_data(num_users_to_generate)
