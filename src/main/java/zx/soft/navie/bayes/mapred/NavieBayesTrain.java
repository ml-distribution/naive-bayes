package zx.soft.navie.bayes.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zx.soft.navie.bayes.utils.HDFSUtils;

/**
 * 训练模块
 * 
 * @author wanggang
 *
 */
public class NavieBayesTrain extends Configured implements Tool {

	/**
	 * 运行作业
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Configuration classifyConf = new Configuration();

		Path trainData = new Path(conf.get("train"));
		Path output = new Path(conf.get("output"));
		int numReducers = conf.getInt("reducers", 10);
		Path distCache = new Path(output.getParent(), "cache");
		Path model = new Path(output.getParent(), "model");

		// Job 1a: 提取每个词语的信息
		HDFSUtils.delete(conf, model);
		Job trainWordJob = new Job(conf, "navie-bayes-word-train");
		trainWordJob.setJarByClass(NavieBayesTrain.class);
		trainWordJob.setNumReduceTasks(numReducers);
		trainWordJob.setMapperClass(TrainWordMapper.class);
		trainWordJob.setReducerClass(TrainWordReducer.class);

		trainWordJob.setInputFormatClass(TextInputFormat.class);
		trainWordJob.setOutputFormatClass(TextOutputFormat.class);

		trainWordJob.setMapOutputKeyClass(Text.class);
		trainWordJob.setMapOutputValueClass(Text.class);
		trainWordJob.setOutputKeyClass(Text.class);
		trainWordJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(trainWordJob, trainData);
		FileOutputFormat.setOutputPath(trainWordJob, model);

		if (!trainWordJob.waitForCompletion(true)) {
			System.err.println("ERROR: Word training failed!");
			return 1;
		}

		classifyConf.setLong(NavieBayesDistribute.UNIQUE_WORDS,
				trainWordJob.getCounters().findCounter(NavieBayesDistribute.NB_COUNTERS.UNIQUE_WORDS).getValue());

		// Job 1b: 按照类别进行统计计算
		HDFSUtils.delete(conf, distCache);
		Job trainCateJob = new Job(conf, "navie-bayes-cate-train");
		trainCateJob.setJarByClass(NavieBayesTrain.class);
		trainCateJob.setNumReduceTasks(numReducers);
		trainCateJob.setMapperClass(TrainCateMapper.class);
		trainCateJob.setReducerClass(TrainCateReducer.class);

		trainCateJob.setInputFormatClass(TextInputFormat.class);
		trainCateJob.setOutputFormatClass(TextOutputFormat.class);

		trainCateJob.setMapOutputKeyClass(Text.class);
		trainCateJob.setMapOutputValueClass(IntWritable.class);
		trainCateJob.setOutputKeyClass(Text.class);
		trainCateJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(trainCateJob, trainData);
		FileOutputFormat.setOutputPath(trainCateJob, distCache);

		if (!trainCateJob.waitForCompletion(true)) {
			System.err.println("ERROR: Cate training failed!");
			return 1;
		}

		classifyConf.setLong(NavieBayesDistribute.UNIQUE_LABELS,
				trainCateJob.getCounters().findCounter(NavieBayesDistribute.NB_COUNTERS.UNIQUE_LABELS).getValue());
		classifyConf.setLong(NavieBayesDistribute.TOTAL_SAMPLES,
				trainCateJob.getCounters().findCounter(NavieBayesDistribute.NB_COUNTERS.TOTAL_SAMPLES).getValue());

		return 0;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new NavieBayesTrain(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
