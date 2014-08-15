
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

从DB中单张表读数据进行分词：
======

```java
运行脚本：

hadoop jar naive-bayes-jar-with-dependencies.jar dbToWordsProcess -D tableName=mysql-tablename  -D processData=process-words
```

输出：
鼓乐喧天        5
鼓乐声  6
鼓室    3
鼓掌欢迎        13


将多张表分词后结果合并：
======

```java
运行脚本：

hadoop jar naive-bayes-jar-with-dependencies.jar mergeWordsProcess -D sourceData=process-words -D dstData=all-words
```

输出：
不告而辞        2
不周    1202
不回    37425
不在乎  54829
不在家  14280
不在意  11513
不在此列        128


将广告相关微博语料进行分词：
======

```java
运行脚本：

hadoop jar naive-bayes-jar-with-dependencies.jar dbToWordsProcess -D tableName=mysql-tablename  -D processData=advertisement-words
```

输出：
一键	119
下载	119
信息流	119
分享	404435
升级	119
听歌	119
图片	404435
地址	119
好友	238
