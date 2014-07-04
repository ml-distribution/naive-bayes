#!/bin/bash

for i in `cat tablenames`;
    do
        echo $i;    
        hadoop jar navie-bayes-jar-with-dependencies.jar dbToHdfsDataProcess -D tableName=$i -D processData=weibo-data/$i
    done;
