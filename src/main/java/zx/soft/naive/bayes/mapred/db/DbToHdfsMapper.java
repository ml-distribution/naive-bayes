package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DbToHdfsMapper extends Mapper<LongWritable, DbInputWritable, LongWritable, DbInputWritable> {

	@Override
	protected void map(LongWritable key, DbInputWritable value, Context context) throws IOException,
			InterruptedException {

		context.write(key, value);

	}

}
