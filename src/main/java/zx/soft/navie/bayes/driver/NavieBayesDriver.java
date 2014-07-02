package zx.soft.navie.bayes.driver;

import org.apache.hadoop.util.ProgramDriver;

import zx.soft.navie.bayes.mapred.NavieBayesDistribute;
import zx.soft.navie.bayes.mapred.db.DbToHdfsDataProcess;
import zx.soft.navie.bayes.mapred.db.HdfsToDBProcess;
import zx.soft.navie.bayes.mapred.txt.TxtToHdfsDataProcess;
import zx.soft.navie.bayes.simple.NavieBayesSimple;

/**
 * 驱动类
 * 
 * @author wanggang
 *
 */
public class NavieBayesDriver {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("navieBayesSimple", NavieBayesSimple.class, "简单Navie-Bayes实现");
			pgd.addClass("txtToHdfsDataProcess", TxtToHdfsDataProcess.class, "Txt数据处理，并存储到HDFS中");
			pgd.addClass("dbToHdfsDataProcess", DbToHdfsDataProcess.class, "DB数据处理，并存储到HDFS中");
			pgd.addClass("navieBayesDistribute", NavieBayesDistribute.class, "分布式Navie Bayes实现");
			pgd.addClass("hdfsToDBProcess", HdfsToDBProcess.class, "将Navie Bayes计算的结果从HDFS中导入到DB中");
			pgd.driver(args);
			// Success
			exitCode = 0;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		System.exit(exitCode);

	}

}
