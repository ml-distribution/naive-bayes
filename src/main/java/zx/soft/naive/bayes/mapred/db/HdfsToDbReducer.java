package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HdfsToDbReducer extends Reducer<LongWritable, Text, DbOutputWritable, NullWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {

		for (Text value : values) {
			context.write(new DbOutputWritable(key.get(), value.toString()), NullWritable.get());
		}

	}

}