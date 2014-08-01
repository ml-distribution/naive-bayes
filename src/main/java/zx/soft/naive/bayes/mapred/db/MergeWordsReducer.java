package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 输出结果: catei wordi wordj wordk
			 catej wordi wordj wordk
			 
 * @author wanggang
 *
 */

public class MergeWordsReducer extends Reducer<Text, LongWritable,  Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws InterruptedException,
			IOException {
		
		long sum = 0;
		for (LongWritable value : values) {
			sum += value.get();
		}
		context.write(key, new LongWritable(sum));

	}

}