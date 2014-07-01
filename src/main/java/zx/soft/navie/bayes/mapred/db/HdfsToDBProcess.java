package zx.soft.navie.bayes.mapred.db;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zx.soft.navie.bayes.mapred.txt.TxtToHdfsDataProcess;
import zx.soft.navie.bayes.utils.ConfigUtil;
import zx.soft.navie.bayes.utils.HDFSUtils;

/**
 * 将Navie Bayes计算的结果从HDFS中导入到DB中。
 * 
 * @author wanggang
 *
 */
public class HdfsToDBProcess extends Configured implements Tool {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new TxtToHdfsDataProcess(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		int numReduceTasks = conf.getInt("numReduceTasks", 8);

		Path resultPath = new Path(conf.get("outputResult"));
		HDFSUtils.delete(conf, resultPath);

		Properties props = ConfigUtil.getProps("data_db.properties");
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", // driver class
				props.getProperty("db.url"), // db url
				props.getProperty("db.username"), // username
				props.getProperty("db.password")); //password

		Job job = new Job(conf, "Navie-Bayes-Output-DB-Result");
		job.setJarByClass(HdfsToDBProcess.class);
		job.setMapperClass(HdfsToDbMapper.class);
		job.setReducerClass(HdfsToDbReducer.class);

		job.setNumReduceTasks(numReduceTasks);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(DbOutputWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, resultPath);
		job.setOutputFormatClass(DBOutputFormat.class);

		DBOutputFormat.setOutput(job, "nb_result", // output table name
				new String[] { "wid", "cate" } //table columns
				);

		if (!job.waitForCompletion(true)) {
			System.err.println("ERROR: HdfsToDBProcess failed!");
			return 1;
		}

		return 0;

	}

}
