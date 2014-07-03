package zx.soft.navie.bayes.mapred.core;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 处理模型，基本的IdentityMapper，原样输出
 * 
 * @author wgybzb
 *
 */
public class JoinModelWordMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		context.write(key, value);
	}

}
