#!/bin/bash

for i in `cat tablenames`;
    do
        echo $i;    
        hadoop jar naive-bayes-jar-with-dependencies.jar dbToHdfsDataProcess -D tableName=$i -D processData=weibo-data/$i
    done;
