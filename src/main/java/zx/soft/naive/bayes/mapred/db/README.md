
从DB中读数据进行分布式NB分类的三种方式：
======

1. 从DB中读数据处理成分词后的数据，输出到HDFS中，再进行NB计算，最后结果输出到HDFS中；
2. 从DB中读数据，直接进行分词和NB计算，将最后结果输出到HDFS中；
3. 从DB中读数据，直接进行分词和NB计算，将最后结果输出到DB中；

**** 优缺点

1. 方法一：需要存储中间数据，但是计算很快；
2. 方法二：没有中间数据，但是在读取数据阶段，受MySQL读取性能的影响；
3. 方法三：没有中间数据，但是在读写数据阶段，受MySQL读写性能的影响；

> **备注:** 需要将JDBC驱动jar包拷贝到hadoop的lib目录下，一般使用mysql-connector-java-5.1.6.jar。

```java
运行脚本：

hadoop jar naive-bayes-jar-with-dependencies.jar dbToHdfsDataProcess -D tableName=mysql-tablename -D processData=process-data
```