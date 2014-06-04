package zx.soft.navie.bayes.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 处理模型，基本的IdentityMapper。
 * @author wgybzb
 *
 */
public class JoinModelMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		context.write(key, value);
	}

}
