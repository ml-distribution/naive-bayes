package zx.soft.navie.bayes.mapred;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
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
 * 测试模块
 * 
 * @author wanggang
 *
 */
public class NavieBayesTest extends Configured implements Tool {

	/**
	 * 运行作业
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Configuration classifyConf = new Configuration();

		Path testdata = new Path(conf.get("test"));
		Path output = new Path(conf.get("output"));
		int numReducers = conf.getInt("reducers", 10);
		Path distCache = new Path(output.getParent(), "cache");
		Path model = new Path(output.getParent(), "model");
		// 中间数据，运行完需要删除
		Path joined = new Path(output.getParent(), "joined");

		// Job 2: 在Reduce端，将测试数据和模型联接起来
		HDFSUtils.delete(conf, joined);
		Job joinJob = new Job(conf, "navie-bayes-test-prep");
		joinJob.setJarByClass(NavieBayesTest.class);
		joinJob.setNumReduceTasks(numReducers);
		MultipleInputs.addInputPath(joinJob, model, KeyValueTextInputFormat.class, JoinModelMapper.class);
		MultipleInputs.addInputPath(joinJob, testdata, TextInputFormat.class, JoinForecastMapper.class);
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

		// Job 3: 分类阶段
		HDFSUtils.delete(classifyConf, output);

		// 添加到分布式缓存
		FileSystem fs = distCache.getFileSystem(classifyConf);
		Path pathPattern = new Path(distCache, "part-r-[0-9]*");
		FileStatus[] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), classifyConf);
		}
		Job classify = new Job(classifyConf, "navie-bayes-classify");
		classify.setJarByClass(NavieBayesTest.class);
		classify.setNumReduceTasks(numReducers);

		classify.setMapperClass(ClassifyMapper.class);
		classify.setReducerClass(ClassifyTestReducer.class);

		classify.setInputFormatClass(KeyValueTextInputFormat.class);
		classify.setOutputFormatClass(TextOutputFormat.class);

		classify.setMapOutputKeyClass(LongWritable.class);
		classify.setMapOutputValueClass(Text.class);
		classify.setOutputKeyClass(LongWritable.class);
		classify.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(classify, joined);
		FileOutputFormat.setOutputPath(classify, output);

		if (!classify.waitForCompletion(true)) {
			System.err.println("ERROR: Classification failed!");
			return 1;
		}

		// 删除中间数据
		HDFSUtils.delete(conf, joined);

		// Last job: 读取输出文件，并对分类概率列表排序

		int correct = 0;
		int total = 0;
		pathPattern = new Path(output, "part-r-[0-9]*");
		FileStatus[] results = fs.globStatus(pathPattern);
		for (FileStatus result : results) {
			FSDataInputStream input = fs.open(result.getPath());
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String[] pieces = line.split("\t");
				correct += (Integer.parseInt(pieces[1]) == 1 ? 1 : 0);
				total++;
			}
			IOUtils.closeStream(in);
		}

		System.out.println(String.format("%s/%s, accuracy %.2f", correct, total,
				((double) correct / (double) total) * 100.0));
		return 0;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new NavieBayesTest(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}