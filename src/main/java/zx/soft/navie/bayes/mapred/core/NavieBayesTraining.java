package zx.soft.navie.bayes.mapred.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.navie.bayes.utils.HDFSUtils;

/**
 * 配置Naive Bayes的训练和测试作业
 * 
 * @author wgybzb
 *
 */
public class NavieBayesTraining extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(NavieBayesTraining.class);

	/**
	 * 运行作业
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Configuration modelConf = new Configuration();
		// 配置reducer个数，可选
		int numReducers = conf.getInt("reducers", 20);
		// 获取训练数据路径和输出的模型路径
		Path trainData = new Path(conf.get("train"));
		Path model = new Path(conf.get("model"));
		// 利用训练数据计算模型时的中间数据，和model同级
		Path wordsCache = new Path(model.getParent(), "words");
		Path catesCache = new Path(model.getParent(), "cates");
		Path modelCate = new Path(model, "modelCates");
		Path modelWord = new Path(model, "modelWords");
		// 删除已有路径
		HDFSUtils.delete(conf, wordsCache);
		HDFSUtils.delete(conf, catesCache);
		HDFSUtils.delete(modelConf, modelCate);
		HDFSUtils.delete(modelConf, modelWord);

		/**
		 * 作业1：提取训练集合中每个词语的信息，即每个词语在每个类别中出现的次数。
		 * 
		 * 输出示例：“词语 cate-i:10 cate-j:9 cate-k:41”
		 */
		Job trainWordJob = new Job(conf, "Navie-Bayes-Training-Words");
		trainWordJob.setJarByClass(NavieBayesTraining.class);
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
		FileOutputFormat.setOutputPath(trainWordJob, wordsCache);

		if (!trainWordJob.waitForCompletion(true)) {
			logger.error("ERROR: Word training failed!");
			return 1;
		}
		// 将不同词语总数记录到分类模型计算的配置文件中
		modelConf.setLong(NavieBayesConstant.UNIQUE_WORDS,
				trainWordJob.getCounters().findCounter(NavieBayesConstant.NB_COUNTERS.UNIQUE_WORDS).getValue());

		/**
		 * 作业2：提取训练集合中的每个类别的信息，即每个类别的文档数和词语量
		 * 
		 * 输出示例：“cate-i 92906:1759397”
		 */
		Job trainCateJob = new Job(conf, "Navie-Bayes-Training-Cates");
		trainCateJob.setJarByClass(NavieBayesTraining.class);
		trainCateJob.setNumReduceTasks(1);
		trainCateJob.setMapperClass(TrainCateMapper.class);
		trainCateJob.setReducerClass(TrainCateReducer.class);
		trainCateJob.setInputFormatClass(TextInputFormat.class);
		trainCateJob.setOutputFormatClass(TextOutputFormat.class);
		trainCateJob.setMapOutputKeyClass(Text.class);
		trainCateJob.setMapOutputValueClass(IntWritable.class);
		trainCateJob.setOutputKeyClass(Text.class);
		trainCateJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(trainCateJob, trainData);
		FileOutputFormat.setOutputPath(trainCateJob, catesCache);

		if (!trainCateJob.waitForCompletion(true)) {
			System.err.println("ERROR: Cate training failed!");
			return 1;
		}
		// 将不同类别总数和样本总数记录到分类模型计算的配置文件中
		modelConf.setLong(NavieBayesConstant.UNIQUE_CATES,
				trainCateJob.getCounters().findCounter(NavieBayesConstant.NB_COUNTERS.UNIQUE_CATES).getValue());
		modelConf.setLong(NavieBayesConstant.TOTAL_SAMPLES,
				trainCateJob.getCounters().findCounter(NavieBayesConstant.NB_COUNTERS.TOTAL_SAMPLES).getValue());
		// 将类别信息添加到分布式缓存
		FileSystem fs = catesCache.getFileSystem(modelConf);
		Path pathPattern = new Path(catesCache, "part-r-[0-9]*");
		FileStatus[] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), modelConf);
		}

		/*
		 * 作业3：模型计算，cate出现的概率
		 */
		Job modelCateJob = new Job(modelConf, "Navie-Bayes-Modeling-Cates");
		modelCateJob.setJarByClass(NavieBayesTraining.class);
		modelCateJob.setNumReduceTasks(1);
		modelCateJob.setMapperClass(ModelCateMapper.class);
		modelCateJob.setReducerClass(ModelCateReducer.class);
		modelCateJob.setInputFormatClass(KeyValueTextInputFormat.class);
		modelCateJob.setOutputFormatClass(TextOutputFormat.class);
		modelCateJob.setMapOutputKeyClass(Text.class);
		modelCateJob.setMapOutputValueClass(Text.class);
		modelCateJob.setOutputKeyClass(Text.class);
		modelCateJob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(modelCateJob, catesCache);
		FileOutputFormat.setOutputPath(modelCateJob, modelCate);

		if (!modelCateJob.waitForCompletion(true)) {
			System.err.println("ERROR: Model-cate computing failed!");
			return 1;
		}

		/*
		 * 作业4：模型计算，word在cate下出现的概率
		 */
		Job modelWordJob = new Job(modelConf, "Navie-Bayes-Modeling-Words");
		modelWordJob.setJarByClass(NavieBayesTraining.class);
		modelWordJob.setNumReduceTasks(numReducers);
		modelWordJob.setMapperClass(ModelWordMapper.class);
		modelWordJob.setReducerClass(ModelWordReducer.class);
		modelWordJob.setInputFormatClass(KeyValueTextInputFormat.class);
		modelWordJob.setOutputFormatClass(TextOutputFormat.class);
		modelWordJob.setMapOutputKeyClass(Text.class);
		modelWordJob.setMapOutputValueClass(Text.class);
		modelWordJob.setOutputKeyClass(Text.class);
		modelWordJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(modelWordJob, wordsCache);
		FileOutputFormat.setOutputPath(modelWordJob, modelWord);

		if (!modelWordJob.waitForCompletion(true)) {
			System.err.println("ERROR: Model-word computing failed!");
			return 1;
		}

		return 0;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new NavieBayesTraining(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}