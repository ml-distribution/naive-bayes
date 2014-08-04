package zx.soft.naive.bayes.mapred.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zx.soft.naive.bayes.mapred.input.IgnoreEofSequenceFileInputFormat;
import zx.soft.naive.bayes.utils.HDFSUtils;

/**
 * 对DbToWordsProcesss输出的多个分词目录进行合并，对词语出现的频数进行累加
 * 
 * @author frank
 *
 */

public class MergeWordsProcess extends Configured implements Tool {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new MergeWordsProcess(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		int numReduceTasks = conf.getInt("numReduceTasks", 10);

		Path sourceDataPath = new Path(conf.get("sourceData"));
		Path dstDataPath = new Path(conf.get("dstData"));

		HDFSUtils.delete(conf, dstDataPath);

		Job job = new Job(conf, "Naive-Bayes-MergeWordsProcess");
		job.setJarByClass(MergeWordsProcess.class);
		job.setMapperClass(MergeWordsMapper.class);
		job.setReducerClass(MergeWordsReducer.class);
		// 输入的文件是序列化文件，所以需要设置
		job.setInputFormatClass(IgnoreEofSequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReduceTasks);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/**
		 * 输入文件目录有两种形式：
		 * 1.只含待处理文件  all-words
		 * 2.只含待处理子目录，子目录里还有待处理文件  process-words
		 */
		FileSystem hdfs = FileSystem.get(conf);
		FileStatus[] inputs = hdfs.listStatus(sourceDataPath);
		String inputsType = "F"; //输入目录只含文件
		for (FileStatus input : inputs) {
			if (input.isDir()) {
				inputsType = "D"; //输入目录包含子目录
				break;
			}
		}
		if (inputsType.equals("F")) {
			FileInputFormat.setInputPaths(job, sourceDataPath);
		} else if (inputsType.equals("D")) {
			FileInputFormat.setInputPaths(job, sourceDataPath + "/*/");
		}

		FileOutputFormat.setOutputPath(job, dstDataPath);

		if (!job.waitForCompletion(true)) {
			System.err.println("ERROR: TxtToHdfsDataProcess failed!");
			return 1;
		}
		return 0;
	}

}
