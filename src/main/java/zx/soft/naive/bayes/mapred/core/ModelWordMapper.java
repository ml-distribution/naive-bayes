package zx.soft.naive.bayes.mapred.core;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author wanggang
 *
 */
public class ModelWordMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		context.write(key, value);
	}

}
