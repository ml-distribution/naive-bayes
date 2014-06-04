package zx.soft.navie.bayes.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

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

/**
 * 配置Naive Bayes的训练和测试作业
 * @author wgybzb
 *
 */
public class Controller extends Configured implements Tool {

	public static final double ALPHA = 1.0;

	static enum NB_COUNTERS {
		TOTAL_DOCS, UNIQUE_WORDS, UNIQUE_LABELS
	}

	// 文档数
	public static final String TOTAL_DOCS = "edu.cmu.bigdata.shannon.total_docs";
	// 词数
	public static final String UNIQUE_WORDS = "edu.cmu.bigdata.shannon.vocabulary_size";
	// 类别数
	public static final String UNIQUE_LABELS = "edu.cmu.bigdata.shannon.unique_labels";

	/**
	 * 将一个文档下的词汇数组转换成向量
	 * @param words
	 * @return
	 */
	public static Vector<String> tokenizeDoc(String[] words) {
		Vector<String> tokens = new Vector<String>();
		for (int i = 1; i < words.length; i++) {
			words[i] = words[i].replaceAll("\\W", "");
			if (words[i].length() > 0) {
				tokens.add(words[i]);
			}
		}
		return tokens;
	}

	/**
	 * 将一个文档下的类别数组转换成向量
	 * @param labels
	 * @return
	 */
	public static Vector<String> tokenizeLabels(String labels) {
		String[] tokens = labels.split(",");
		Vector<String> retval = new Vector<String>();
		for (String token : tokens) {
			retval.add(token);
		}
		return retval;
	}

	/**
	 * 删除HDFS中的某个目录
	 * @param conf
	 * @param path
	 * @throws IOException
	 */
	public static void delete(Configuration conf, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	/**
	 * 运行作业
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Configuration classifyConf = new Configuration();

		Path traindata = new Path(conf.get("train"));
		Path testdata = new Path(conf.get("test"));
		Path output = new Path(conf.get("output"));
		int numReducers = conf.getInt("reducers", 10);
		Path distCache = new Path(output.getParent(), "cache");
		Path model = new Path(output.getParent(), "model");
		Path joined = new Path(output.getParent(), "joined");

		// Job 1a: 提取每个词语的信息
		Controller.delete(conf, model);
		Job trainWordJob = new Job(conf, "zxsoft-nb-wordtrain");
		trainWordJob.setJarByClass(Controller.class);
		trainWordJob.setNumReduceTasks(numReducers);
		trainWordJob.setMapperClass(TrainWordMapper.class);
		trainWordJob.setReducerClass(TrainWordReducer.class);

		trainWordJob.setInputFormatClass(TextInputFormat.class);
		trainWordJob.setOutputFormatClass(TextOutputFormat.class);

		trainWordJob.setMapOutputKeyClass(Text.class);
		trainWordJob.setMapOutputValueClass(Text.class);
		trainWordJob.setOutputKeyClass(Text.class);
		trainWordJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(trainWordJob, traindata);
		FileOutputFormat.setOutputPath(trainWordJob, model);

		if (!trainWordJob.waitForCompletion(true)) {
			System.err.println("ERROR: Word training failed!");
			return 1;
		}

		classifyConf.setLong(Controller.UNIQUE_WORDS,
				trainWordJob.getCounters().findCounter(Controller.NB_COUNTERS.UNIQUE_WORDS).getValue());

		// Job 1b: 按照类别进行统计计算
		Controller.delete(conf, distCache);
		Job trainCateJob = new Job(conf, "zxsoft-nb-catetrain");
		trainCateJob.setJarByClass(Controller.class);
		trainCateJob.setNumReduceTasks(numReducers);
		trainCateJob.setMapperClass(TrainCateMapper.class);
		trainCateJob.setReducerClass(TrainCateReducer.class);

		trainCateJob.setInputFormatClass(TextInputFormat.class);
		trainCateJob.setOutputFormatClass(TextOutputFormat.class);

		trainCateJob.setMapOutputKeyClass(Text.class);
		trainCateJob.setMapOutputValueClass(IntWritable.class);
		trainCateJob.setOutputKeyClass(Text.class);
		trainCateJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(trainCateJob, traindata);
		FileOutputFormat.setOutputPath(trainCateJob, distCache);

		if (!trainCateJob.waitForCompletion(true)) {
			System.err.println("ERROR: Cate training failed!");
			return 1;
		}

		classifyConf.setLong(Controller.UNIQUE_LABELS,
				trainCateJob.getCounters().findCounter(Controller.NB_COUNTERS.UNIQUE_LABELS).getValue());
		classifyConf.setLong(Controller.TOTAL_DOCS,
				trainCateJob.getCounters().findCounter(Controller.NB_COUNTERS.TOTAL_DOCS).getValue());

		// Job 2: 在Reduce端，将测试数据和模型联接起来
		Controller.delete(conf, joined);
		Job joinJob = new Job(conf, "zxsoft-nb-testprep");
		joinJob.setJarByClass(Controller.class);
		joinJob.setNumReduceTasks(numReducers);
		MultipleInputs.addInputPath(joinJob, model, KeyValueTextInputFormat.class, JoinModelMapper.class);
		MultipleInputs.addInputPath(joinJob, testdata, TextInputFormat.class, JoinTestMapper.class);
		joinJob.setReducerClass(JoinReducer.class);

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
		Controller.delete(classifyConf, output);

		// 添加到分布式缓存
		FileSystem fs = distCache.getFileSystem(classifyConf);
		Path pathPattern = new Path(distCache, "part-r-[0-9]*");
		FileStatus[] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), classifyConf);
		}
		Job classify = new Job(classifyConf, "zxsoft-nb-classify");
		classify.setJarByClass(Controller.class);
		classify.setNumReduceTasks(numReducers);

		classify.setMapperClass(ClassifyMapper.class);
		classify.setReducerClass(ClassifyReducer.class);

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
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Controller(), args);
		System.exit(exitCode);
	}

}