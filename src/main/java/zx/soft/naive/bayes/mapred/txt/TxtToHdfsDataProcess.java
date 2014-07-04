package zx.soft.naive.bayes.mapred.txt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import zx.soft.naive.bayes.utils.HDFSUtils;

public class TxtToHdfsDataProcess extends Configured implements Tool {

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

		Path sourceDataPath = new Path(conf.get("sourceData"));
		Path dstDataPath = new Path(conf.get("processData"));

		HDFSUtils.delete(conf, dstDataPath);

		Job job = new Job(conf, "Navie-Bayes-Txt-DataProcess");
		job.setJarByClass(TxtToHdfsDataProcess.class);
		job.setMapperClass(TxtToHdfsMapper.class);
		job.setReducerClass(TxtToHdfsReducer.class);

		job.setNumReduceTasks(numReduceTasks);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, sourceDataPath);
		FileOutputFormat.setOutputPath(job, dstDataPath);

		if (!job.waitForCompletion(true)) {
			System.err.println("ERROR: TxtToHdfsDataProcess failed!");
			return 1;
		}
		return 0;
	}

}
