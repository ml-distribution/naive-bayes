
    

1.训练模型：

hadoop jar navie-bayes-jar-with-dependencies.jar navieBayesTraining -D train=output-dataProcessing -D model=train-model

2.预测分类：

hadoop jar navie-bayes-jar-with-dependencies.jar navieBayesForecast -D forecast=weibo-data -D model=train-model -D output=output-result




