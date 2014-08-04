package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DbToWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static int sum = 0;

	@Override
	protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {

		sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(word, new IntWritable(sum));
	}

}
