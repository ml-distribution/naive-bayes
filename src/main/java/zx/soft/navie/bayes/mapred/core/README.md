
    

1.训练模型：

hadoop jar navie-bayes-jar-with-dependencies.jar navieBayesTraining -D train=output-dataProcessing -D model=train-model

2.预测分类：

hadoop jar navie-bayes-jar-with-dependencies.jar navieBayesForecast -D forecast=weibo-data -D model=train-model -D output=output-result

预测阶段Bug：

14/07/03 17:13:32 INFO mapred.JobClient: Task Id : attempt_201406270905_0113_r_000016_0, Status : FAILED
Error: Java heap space



