package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DbToWordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private static long sum = 0;

	@Override
	protected void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {

		sum = 0;

		for (LongWritable value : values) {
			sum += value.get();
		}
		context.write(word, new LongWritable(sum));
	}

}
