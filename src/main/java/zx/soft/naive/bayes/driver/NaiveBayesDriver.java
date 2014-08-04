package zx.soft.naive.bayes.driver;

import org.apache.hadoop.util.ProgramDriver;

import zx.soft.naive.bayes.mapred.NaiveBayesDistribute;
import zx.soft.naive.bayes.mapred.core.NaiveBayesForecast;
import zx.soft.naive.bayes.mapred.core.NavieBayesTraining;
import zx.soft.naive.bayes.mapred.db.DbToHdfsDataProcess;
import zx.soft.naive.bayes.mapred.db.DbToWordsProcess;
import zx.soft.naive.bayes.mapred.db.HdfsToDBProcess;
import zx.soft.naive.bayes.mapred.db.MergeWordsProcess;
import zx.soft.naive.bayes.mapred.txt.TxtToHdfsDataProcess;
import zx.soft.naive.bayes.mapred.txt.VerifyTxtToHdfsDataProcess;
import zx.soft.naive.bayes.simple.NaiveBayesSimple;
import zx.soft.naive.bayes.web.NaiveBayesServer;

/**
 * 驱动类
 * 
 * @author wanggang
 *
 */
public class NaiveBayesDriver {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("naiveBayesSimple", NaiveBayesSimple.class, "简单Naive-Bayes实现");
			pgd.addClass("txtToHdfsDataProcess", TxtToHdfsDataProcess.class, "Txt数据处理，并存储到HDFS中");
			pgd.addClass("dbToHdfsDataProcess", DbToHdfsDataProcess.class, "DB数据处理，并存储到HDFS中");
			pgd.addClass("dbToWordsProcess", DbToWordsProcess.class, "将单张数据表进行分词，并存储到HDFS中");
			pgd.addClass("mergeWordsProcess", MergeWordsProcess.class, "合并单张数据表分词的结果，并存储到HDFS中");
			pgd.addClass("verifyTxtToHdfsDataProcess", VerifyTxtToHdfsDataProcess.class, "从训练数据中抽取一部分作为验证数据");
			pgd.addClass("naiveBayesDistribute", NaiveBayesDistribute.class, "分布式Naive-Bayes实现");
			pgd.addClass("hdfsToDBProcess", HdfsToDBProcess.class, "将Naive-Bayes计算的结果从HDFS中导入到DB中");
			pgd.addClass("naiveBayesTraining", NavieBayesTraining.class, "Naivve-Bayes模型训练");
			pgd.addClass("naiveBayesForecast", NaiveBayesForecast.class, "Naive-Bayes分类预测");
			pgd.addClass("naiveBayesServer", NaiveBayesServer.class, "Naive-Bayes分类的Web服务接口");
			pgd.driver(args);
			// Success
			exitCode = 0;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		System.exit(exitCode);

	}

}