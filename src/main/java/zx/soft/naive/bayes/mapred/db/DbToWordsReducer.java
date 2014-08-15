package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DbToWordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private static final int MIN_COUNT = 1;

	private static int sum = 0;

	@Override
	protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {

		sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}
		// 输出频次大于一定值的词语
		if (sum >= MIN_COUNT) {
			context.write(word, new IntWritable(sum));
		}
	}

}
