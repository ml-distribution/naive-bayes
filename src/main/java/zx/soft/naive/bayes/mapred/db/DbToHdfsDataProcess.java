package zx.soft.naive.bayes.mapred.db;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zx.soft.naive.bayes.utils.ConfigUtil;
import zx.soft.naive.bayes.utils.HDFSUtils;

public class DbToHdfsDataProcess extends Configured implements Tool {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new DbToHdfsDataProcess(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		/**
		 * 设置Mapper输出压缩
		 */
		conf.setBoolean("mapred.compress.map.output", true); // 开起map输出压缩
		conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class); // 设置压缩算法

		int numReduceTasks = conf.getInt("numReduceTasks", 20);

		// 由于数据表很多，而每次只能处理一个数据表，所以需要批量处理多个数据表
		String tablename = conf.get("tableName");

		Path dstDataPath = new Path(conf.get("processData"));
		HDFSUtils.delete(conf, dstDataPath);

		Properties props = ConfigUtil.getProps("data_db.properties");
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", // driver class
				props.getProperty("db.url"), // db url
				props.getProperty("db.username"), // username
				props.getProperty("db.password")); //password

		Job job = new Job(conf, "Naive-Bayes-DB-DataProcess");
		job.setJarByClass(DbToHdfsDataProcess.class);
		job.setMapperClass(DbToHdfsMapper.class);
		job.setReducerClass(DbToHdfsReducer.class);

		job.setNumReduceTasks(numReduceTasks);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DbInputWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, dstDataPath);

		/**
		 * 设置输出压缩
		 */
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		job.setInputFormatClass(DBInputFormat.class);
		// 是否可设置多个数据表？
		DBInputFormat.setInput(job, DbInputWritable.class, tablename, //input table name
				null, null, new String[] { "wid", "text" } // table columns
				);

		if (!job.waitForCompletion(true)) {
			System.err.println("ERROR: DbToHdfsDataProcess failed!");
			return 1;
		}

		return 0;

	}

}
