import time
import random
import pandas as pd
import pandera as pa
import numpy as np
import math
import string
import sys
from datetime import datetime
from faker import Faker
from faker.providers.person.en import Provider as personProvider
from faker.providers.job.en_US import Provider as jobProvider
from faker.providers.internet.en_US import Provider as urlProvider
from geopy.geocoders import Nominatim
from pandera import Column, DataFrameSchema, Check, Index


countries = ["Canada","US","UK"]
video_span = ["Supershort","Short","Medium","Lengthy","Tiring","supershort","short","medium","lengthy","tiring"]

#sequentially generated unique video ids starting from 1
def sequential_video_ids(size) :
    ids = np.array([i for i in range(1,size+1)])
    return ids
#sequential_video_ids(5)

# Generating random title names for videos..
def random_title_names(size) :
    names = []
    Faker.seed(0)
    faker = Faker()
    for _ in range(size):
        names.append(faker.company())
    return names
#random_channel_names(10)

def random_locations(size, p=None):
    if not p:
        p = (0.3,0.5,0.2)
        return np.random.choice(countries, size=size, p=p)
#random_locations(20)

# Links countries to coordinates
country_to_coordinates = {}
geolocator = Nominatim(user_agent="tars")
for country in countries :
    country_to_coordinates[country] = (geolocator.geocode(country).latitude, geolocator.geocode(country).longitude)

def random_durations_bucket(size, p=None):
    if not p:
        p = (0.18,0.20,0.15,0.15,0.10,0.02,0.05,0.05,0.05,0.05)
        return np.random.choice(video_span, size=size, p=p)

#random_durations(20)

#Links video length to video durations.
def random_duration(duration_bucket):
    v = duration_bucket
    start = 0
    end = 50
    if v=="Supershort":
        start = 1
        end = 5
    elif v=="Short":
        start = 6
        end = 10
    elif v=="Medium":
        start = 11
        end = 15
    elif v=="Lengthy":
        start = 16
        end = 30
    elif v=="Tiring":
        start = 31
        end = 50
    elif v=="supershort":
        start = 1
        end = 5
    elif v=="short":
        start = 6
        end = 10
    elif v=="medium":
        start = 11
        end = 15
    elif v=="lengthy":
        start = 16
        end = 30
    elif v=="tiring":
        start = 31
        end = 50
    return random.randint(start, end)

def random_categories(size, p=None):
    categories =  ["Action/Adventure", "Anime/Animation","Autos & Vehicles","Classics","Comedy","Documentary","Drama","Education","Entertainment","Family","Film & Animation","Foreign","Gaming","Horror","Howto & Style","Movies","Music","News & Politics","Nonprofits & Activism","People & Blogs","Pets & Animals","Sci-Fi/Fantasy","Science & Technology","Short Movies","Shows","Sports","Thriller","Trailers","Travel & Events","Videoblogging"]
    return np.random.choice(categories, size=size, p=p)

def random_dates(start, end, size):
    # Unix timestamp is in nanoseconds by default, so divide it by 24*60*60*10**9 to convert to days.
    start_u = start.value//10**9
    end_u = end.value//10**9
    return pd.to_datetime(np.random.randint(start_u, end_u, size), unit='s')

def video_resolution(size):
    #Randomly generating video resolution
    resolutions =  ["144p", "240p","360p","480p","720p","1080p"]
    return np.random.choice(resolutions, size=size)

def generate_video_data(size, isEmpty):
    df = pd.DataFrame(columns=['video_id','title', 'country','lat','long', 'duration_bucket', 'duration','category', 'max_available_resolution','posted_on','url'])
    if isEmpty :
        return df
    df['video_id'] = sequential_video_ids(size)
    df['title'] = random_title_names(size)
    df['country'] = random_locations(size)
    df['lat'] = df['country'].apply(lambda x: country_to_coordinates[x][0]) 
    df['long'] = df['country'].apply(lambda x: country_to_coordinates[x][1])
    df['duration_bucket'] = random_durations_bucket(size)
    df['duration'] = df['duration_bucket'].apply(lambda x: random_duration(x)) 
    df['category'] = random_categories(size)
    df['max_available_resolution'] = video_resolution(size)
    df['posted_on'] = random_dates(start=pd.to_datetime('2018-01-01'), end=pd.to_datetime('2020-12-31'), size=size)
    df['url'] = df['video_id'].apply(lambda x: 'http://youtube.com/videoId={0}'.format(x))

    # Adding noise (null values) to all the columns except video_id 
    df_with_nulls = df[['title', 'country','lat','long', 'duration_bucket', 'duration','category', 'max_available_resolution','posted_on','url']]
    df_with_nulls = df_with_nulls.mask(np.random.random(df_with_nulls.shape) < .007)  
    df[['title', 'country','lat','long', 'duration_bucket', 'duration','category', 'max_available_resolution','posted_on','url']] =  df_with_nulls[['title', 'country','lat','long', 'duration_bucket', 'duration','category', 'max_available_resolution','posted_on','url']]
    
    fake= Faker()
    #Adding additional noise column wise
    df['posted_on']=df['posted_on'].mask(np.random.random(df['posted_on'].shape) <.007,df['posted_on'].apply(lambda x: fake.date_between_dates(date_start=datetime(2017,1,1), date_end=datetime(2021,12,31))))
    df['url'] = df['url'].mask(np.random.random(df['url'].shape) < .007,df['url'].apply(lambda x: 'http://' +''.join(random.choices(string.ascii_letters,k=np.random.randint(low=0,high=20)))+'.com'))
    
    df.to_csv('data/raw-video-data.csv')
    return df
generate_video_data(int(sys.argv[1]),False)

