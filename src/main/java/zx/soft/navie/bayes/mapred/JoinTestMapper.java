package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 准备测试数据，用于分类，输出格式：word——>文档ID,类别列表
 * @author wgybzb
 *
 */
public class JoinTestMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

		String[] words = value.toString().split("\\s+");
		Vector<String> cates = NavieBayesDistribute.tokenizeLabels(words[0]);
		Vector<String> text = NavieBayesDistribute.tokenizeDoc(words);

		StringBuilder labelString = new StringBuilder();
		for (String cate : cates) {
			labelString.append(String.format("%s,", cate));
		}
		String output = labelString.toString();

		// word——>文档ID,类别列表
		for (String word : text) {
			context.write(new Text(word),
					new Text(String.format("%s,%s", key.get(), output.substring(0, output.length() - 1))));
		}
	}

}
