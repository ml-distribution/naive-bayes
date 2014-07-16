package zx.soft.naive.bayes.mapred.txt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class VerifyTxtToHdfsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private static String[] texts;
	private static StringBuffer sb;

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		texts = value.toString().split("\\s");
		sb = new StringBuffer();
		for (String t : texts) {
			sb.append(t).append(" ");
		}
		if (sb.length() > 1) {
			context.write(key, new Text(key + " " + sb.subSequence(0, sb.length() - 1)));
		}
	}

}
