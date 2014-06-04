package zx.soft.navie.bayes.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 *
 * Processes the model. Basically the IdentityMapper (since it's apparently
 * nonexistent after Hadoop 0.18, grrr).
 */
public class NBJoinModelMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		context.write(key, value);
	}

}
