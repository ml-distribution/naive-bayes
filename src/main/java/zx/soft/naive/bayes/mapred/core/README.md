
    

1.训练模型：

hadoop jar naive-bayes-jar-with-dependencies.jar naiveBayesTraining -D train=train-data -D model=train-model

2.预测分类：

hadoop jar naive-bayes-jar-with-dependencies.jar naiveBayesForecast -D forecast=weibo-forecast -D model=train-model -D output=output-result

