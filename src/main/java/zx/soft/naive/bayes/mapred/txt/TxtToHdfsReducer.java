package zx.soft.naive.bayes.mapred.txt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 输出结果: catei wordi wordj wordk
			 catej wordi wordj wordk
			 
 * @author wanggang
 *
 */

public class TxtToHdfsReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws InterruptedException,
			IOException {

		for (Text value : values) {
			context.write(NullWritable.get(), value);
		}

	}

}