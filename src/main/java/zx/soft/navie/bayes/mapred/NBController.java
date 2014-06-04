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
 * @author Shannon Quinn
 *
 * Sets up the training and testing jobs for Naive Bayes.
 */
public class NBController extends Configured implements Tool {

	public static final double ALPHA = 1.0;

	static enum NB_COUNTERS {
		TOTAL_DOCS, VOCABULARY_SIZE, UNIQUE_LABELS
	}

	public static final String TOTAL_DOCS = "edu.cmu.bigdata.shannon.total_docs";
	public static final String VOCABULARY_SIZE = "edu.cmu.bigdata.shannon.vocabulary_size";
	public static final String UNIQUE_LABELS = "edu.cmu.bigdata.shannon.unique_labels";

	/**
	 * Taken directly from the homework assignment. Modified to operate
	 * on the words and skip the line with the label text.
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
	 * Tokenizes the labels.
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
	 * Deletes the specified path from HDFS so we can run this multiple times.
	 * @param conf
	 * @param path
	 */
	public static void delete(Configuration conf, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

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

		// Job 1a: Extract information on each word.
		NBController.delete(conf, model);
		Job trainWordJob = new Job(conf, "shannon-nb-wordtrain");
		trainWordJob.setJarByClass(NBController.class);
		trainWordJob.setNumReduceTasks(numReducers);
		trainWordJob.setMapperClass(NBTrainWordMapper.class);
		trainWordJob.setReducerClass(NBTrainWordReducer.class);

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

		classifyConf.setLong(NBController.VOCABULARY_SIZE,
				trainWordJob.getCounters().findCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE).getValue());

		// Job 1b: Tabulate label-based statistics.
		NBController.delete(conf, distCache);
		Job trainLabelJob = new Job(conf, "shannon-nb-labeltrain");
		trainLabelJob.setJarByClass(NBController.class);
		trainLabelJob.setNumReduceTasks(numReducers);
		trainLabelJob.setMapperClass(NBTrainLabelMapper.class);
		trainLabelJob.setReducerClass(NBTrainLabelReducer.class);

		trainLabelJob.setInputFormatClass(TextInputFormat.class);
		trainLabelJob.setOutputFormatClass(TextOutputFormat.class);

		trainLabelJob.setMapOutputKeyClass(Text.class);
		trainLabelJob.setMapOutputValueClass(IntWritable.class);
		trainLabelJob.setOutputKeyClass(Text.class);
		trainLabelJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(trainLabelJob, traindata);
		FileOutputFormat.setOutputPath(trainLabelJob, distCache);

		if (!trainLabelJob.waitForCompletion(true)) {
			System.err.println("ERROR: Label training failed!");
			return 1;
		}

		classifyConf.setLong(NBController.UNIQUE_LABELS,
				trainLabelJob.getCounters().findCounter(NBController.NB_COUNTERS.UNIQUE_LABELS).getValue());
		classifyConf.setLong(NBController.TOTAL_DOCS,
				trainLabelJob.getCounters().findCounter(NBController.NB_COUNTERS.TOTAL_DOCS).getValue());

		// Job 2: Reduce-side join the test dataset with the model.
		NBController.delete(conf, joined);
		Job joinJob = new Job(conf, "shannon-nb-testprep");
		joinJob.setJarByClass(NBController.class);
		joinJob.setNumReduceTasks(numReducers);
		MultipleInputs.addInputPath(joinJob, model, KeyValueTextInputFormat.class, NBJoinModelMapper.class);
		MultipleInputs.addInputPath(joinJob, testdata, TextInputFormat.class, NBJoinTestMapper.class);
		joinJob.setReducerClass(NBJoinReducer.class);

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

		// Job 3: Classification!
		NBController.delete(classifyConf, output);

		// Add to the Distributed Cache.
		FileSystem fs = distCache.getFileSystem(classifyConf);
		Path pathPattern = new Path(distCache, "part-r-[0-9]*");
		FileStatus[] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			DistributedCache.addCacheFile(status.getPath().toUri(), classifyConf);
		}
		Job classify = new Job(classifyConf, "shannon-nb-classify");
		classify.setJarByClass(NBController.class);
		classify.setNumReduceTasks(numReducers);

		classify.setMapperClass(NBClassifyMapper.class);
		classify.setReducerClass(NBClassifyReducer.class);

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

		// Last job: manually read through the output file and 
		// sort the list of classification probabilities.

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
		int exitCode = ToolRunner.run(new NBController(), args);
		System.exit(exitCode);
	}

}