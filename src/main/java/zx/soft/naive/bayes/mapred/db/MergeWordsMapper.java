package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeWordsMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

	@Override
	public void map(Text key, IntWritable value, Context context) throws InterruptedException, IOException {
		context.write(key, value);
	}

}
