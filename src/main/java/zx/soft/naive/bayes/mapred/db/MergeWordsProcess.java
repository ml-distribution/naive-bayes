package zx.soft.naive.bayes.mapred.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
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
		/**
		 * 设置Mapper输出压缩
		 */
		conf.setBoolean("mapred.compress.map.output", true); // 开起map输出压缩
		conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,
				CompressionCodec.class); // 设置压缩算法
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
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		/**
		 * 设置多个路径作为输入文件路径
		 */
		FileInputFormat.setInputPaths(job, sourceDataPath+"/*/");
		FileOutputFormat.setOutputPath(job, dstDataPath);
		/**
		 * 设置输出压缩
		 */
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		if (!job.waitForCompletion(true)) {
			System.err.println("ERROR: TxtToHdfsDataProcess failed!");
			return 1;
		}
		return 0;
	}

}
