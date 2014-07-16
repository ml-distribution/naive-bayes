package zx.soft.naive.bayes.mapred.txt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VerifyTxtToHdfsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		context.write(key, value);
	}

}
