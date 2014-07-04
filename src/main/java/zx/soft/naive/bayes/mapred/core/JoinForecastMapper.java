package zx.soft.naive.bayes.mapred.core;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 预测数据用于分类，输出格式：word——>文档ID
 * 
 * @author wgybzb
 *
 */
public class JoinForecastMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

		String[] words = value.toString().split("\\s+");
		Vector<String> text = TrainsVector.tokenizeDoc(words);
		// word——>文档ID
		for (String word : text) {
			context.write(new Text(word), new Text(words[0]));
		}
	}

}
