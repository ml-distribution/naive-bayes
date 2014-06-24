package zx.soft.navie.bayes.analyzer;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TextAnalyzerMapReduce extends Configured implements Tool {

	private static AnalyzerTool analyzerTool = new AnalyzerTool();

	public static class TextAnalyzerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

			String[] strs = value.toString().split("\\s+");
			if (strs[1].length() > 0) {
				String spiltWords = analyzerTool.analyzerTextToStr(strs[1], "	");
				context.write(key, new Text(strs[0] + "	" + spiltWords));
			}
		}

	}

	public static class TextAnalyzerReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = new Job(getConf(), "Text Analyzer MapReduce");
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TextAnalyzerMapper.class);
		job.setReducerClass(TextAnalyzerReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new TextAnalyzerMapReduce(), args);
		System.exit(exitCode);
	}

}
