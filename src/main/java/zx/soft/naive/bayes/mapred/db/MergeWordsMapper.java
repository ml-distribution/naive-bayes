package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;


public class MergeWordsMapper extends Mapper<Text, LongWritable,  Text, LongWritable> {

	@Override
	public void map(Text key, LongWritable value, Context context) throws InterruptedException, IOException {

		context.write(key, value);
	}

}
