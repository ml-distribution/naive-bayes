Naive Bayes的基本实现以及分布式实现
===========================

分布式部分包含四个MapReduce作业: 
* 两个训练作业 
* 一个用于将训练模型与测试数据集联接的作业
* 一个用于分类的作业


运行作业:

    hadoop jar NavieBayesDriver.jar navieBayesDistribute -D train=/path/to/training/data -D test=/path/to/test/data -D output=/output/dir [-D reducers=10]
    
