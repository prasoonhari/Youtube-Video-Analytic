#!/bin/bash

#first argument = number of users we want to generate
#second argument = number of videos we want to generate as part of our demo dataset
#third argument = scratch directory  under which we create a python environment
#fourth argument = our working peel directory
#using scratch directory to unzip generate_data.zip and create data
cp -u generate_data.zip $3
cd  $3

#unzip scripts
unzip -o generate_data.zip



cd generate_data/

chmod +x generate_category.py
chmod +x generate_raw_user_data.py
chmod +x generate_raw_video_data.py
chmod +x generate_raw_user_meta.py

mkdir -p data
#create virtual env
/share/apps/python/3.8.6/intel/bin/python3.8 -m venv testvenv
source testvenv/bin/activate
#install requirements
pip install pandas
pip install numpy
pip install faker==8.0.0
pip install geopy==2.1.0
pip install pandera==0.6.3
pip install rstr==2.2.6
pip install scipy==1.6.3
pip install matplotlib==3.4.1

# generating seed data using numpy and pandas
python3 generate_category.py
#takes as input the number of users we want to generate
python3 generate_raw_user_data.py $1
#takes as input the number of videos we want to generate
python3 generate_raw_video_data.py $2
# generating user meta. mean number of views for each user is set to 100 for demo run
python3 generate_raw_user_meta.py $1 100

cp -R data $4/data_tars
