#!/bin/bash


#first argument = our working peel directory
#second argument = HDFS directory where we want to store demo run results
hdfs dfs -mkdir $2/


#move to the working directory now
cd $1

hdfs dfs -mkdir $2/clean
hdfs dfs -mkdir $2/wh
#move the raw data folder to HDFS
hdfs dfs -put -f data_tars/ $2

#move meta data to HDFS
hdfs dfs -put -f data_tars/categories.csv $2/clean/
hdfs dfs -put -f data_tars/user-meta-data.csv $2/clean/

#at this point
echo "data generation done"

#we'll be using the same jar file to execute all our spark jobs from here on.


#spark job to clean user data
spark-submit --conf spark.master=yarn --conf spark.yarn.maxAppAttempts=1 --class nyu.bdad.tars.UserDataCleaning tars_1.0.jar "$2/data_tars/raw-user-data.csv" "$2/clean/"

#spark job to clean video data
spark-submit --conf spark.master=yarn --class nyu.bdad.tars.VideoDataCleaning tars_1.0.jar "$2/data_tars/raw-video-data.csv" "$2/clean/"

echo "data cleaning done"

#spark job to generate watch history
spark-submit --conf spark.master=yarn --num-executors 5 --executor-cores 3 --executor-memory 3G --conf spark.yarn.maxAppAttempts=1 --class nyu.bdad.tars.WatchHistoryGeneration tars_1.0.jar "$2/clean/categories.csv" "$2/clean/user/" "$2/clean/video/" "$2/clean/user-meta-data.csv" "$2/wh/" ".05" ".1"

echo "watch history data created"

#spark job to derive statistical insights
spark-submit --conf spark.master=yarn --class nyu.bdad.tars.StatisticalInsights tars_1.0.jar "$2/clean/categories.csv" "$2/wh/user/" "$2/wh/video/" "$2/wh/wh" "$2/stats"

echo "statistical analysis done"

#spark job to derive ml-based insights
spark-submit --class nyu.bdad.tars.NB --master yarn tars_1.0.jar "$2/wh/" "$2/" 1 1 demo

#copy ML results to local directory
hdfs dfs -getmerge "$2/result.txt" ./result.txt

#view results
cat result.txt

echo "Completed"

