package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static int sum = 0;

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException,
			IOException {

		sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(key, new IntWritable(sum));
	}

}