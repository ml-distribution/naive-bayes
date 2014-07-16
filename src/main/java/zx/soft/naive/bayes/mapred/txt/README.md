

```java
运行脚本：

hadoop jar naive-bayes-jar-with-dependencies.jar txtToHdfsDataProcess -D sourceData=weibo-corpora -D processData=train-data

hadoop jar naive-bayes-jar-with-dependencies.jar verifyTxtToHdfsDataProcess -D sourceData=train-data -D processData=verify-data
```