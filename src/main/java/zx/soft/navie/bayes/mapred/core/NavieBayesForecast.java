package zx.soft.navie.bayes.mapred.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zx.soft.navie.bayes.utils.HDFSUtils;

/**
 * 配置Naive Bayes的训练和测试作业
 * 
 * @author wgybzb
 *
 */
public class NavieBayesForecast extends Configured implements Tool {

	/**
	 * 运行作业
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Configuration classifyConf = new Configuration();
		// 配置reducer个数，可选
		int numReducers = conf.getInt("reducers", 20);
		// 获取预测数据路径和模型数据路径
		Path forecastData = new Path(conf.get("forecast"));
		Path model = new Path(conf.get("model"));
		// model数据
		Path modelCate = new Path(model, "modelCates");
		Path modelWord = new Path(model, "modelWords");
		// 数据结果路径
		Path output = new Path(conf.get("output"));
		Path joined = new Path(output.getParent(), "joined");
		// 删除已有路径
		HDFSUtils.delete(conf, joined);
		HDFSUtils.delete(classifyConf, output);

		// 添加类别概率数据到分布式缓存
		FileSystem fs = modelCate.getFileSystem(classifyConf);
		Path pathPattern = new Path(modelCate, "part-r-[0-9]*");
		FileStatus[] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), classifyConf);
		}

		/**
		 * 作业1：将预测数据和模型联接起来
		 */
		HDFSUtils.delete(conf, joined);
		Job joinJob = new Job(conf, "Navie-Bayes-Joined-Forecast-Model");
		joinJob.setJarByClass(NavieBayesForecast.class);
		joinJob.setNumReduceTasks(numReducers);
		MultipleInputs.addInputPath(joinJob, modelWord, KeyValueTextInputFormat.class, JoinModelWordMapper.class);
		MultipleInputs.addInputPath(joinJob, forecastData, TextInputFormat.class, JoinForecastMapper.class);
		joinJob.setReducerClass(JoinForecastReducer.class);
		joinJob.setOutputFormatClass(TextOutputFormat.class);
		joinJob.setMapOutputKeyClass(Text.class);
		joinJob.setMapOutputValueClass(Text.class);
		joinJob.setOutputKeyClass(Text.class);
		joinJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(joinJob, joined);

		if (!joinJob.waitForCompletion(true)) {
			System.err.println("ERROR: Joining failed!");
			return 1;
		}

		/**
		 * 作业2：分类阶段
		 */
		Job classifyJob = new Job(classifyConf, "Navie-Bayes-Classify");
		classifyJob.setJarByClass(NavieBayesForecast.class);
		classifyJob.setNumReduceTasks(numReducers);
		classifyJob.setMapperClass(ForecastMapper.class);
		classifyJob.setReducerClass(ForecastReducer.class);
		classifyJob.setInputFormatClass(KeyValueTextInputFormat.class);
		classifyJob.setOutputFormatClass(TextOutputFormat.class);
		classifyJob.setMapOutputKeyClass(LongWritable.class);
		classifyJob.setMapOutputValueClass(Text.class);
		classifyJob.setOutputKeyClass(LongWritable.class);
		classifyJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(classifyJob, joined);
		FileOutputFormat.setOutputPath(classifyJob, output);

		if (!classifyJob.waitForCompletion(true)) {
			System.err.println("ERROR: Classification failed!");
			return 1;
		}

		return 0;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new NavieBayesForecast(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}