package zx.soft.naive.bayes.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zx.soft.naive.bayes.utils.JavaPattern;

/**
 * 准备测试数据，用于分类，输出格式：word——>文档ID,类别列表
 * 
 * @author wgybzb
 *
 */
public class JoinForecastMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

		String[] words = value.toString().split("\\s+");
		Vector<String> cates = TrainsVector.tokenizeCates(words[0]);
		Vector<String> text = TrainsVector.tokenizeDoc(words);

		if (!JavaPattern.isAllNum(cates.get(0))) { // 用于测试
			StringBuilder cateStr = new StringBuilder();
			for (String cate : cates) {
				cateStr.append(String.format("%s,", cate));
			}
			String output = cateStr.toString();
			// word——>文档ID,类别列表
			for (String word : text) {
				context.write(new Text(word),
						new Text(String.format("%s,%s", key.get(), output.substring(0, output.length() - 1))));
			}
		} else {
			// word——>文档ID(偏移量)
			for (String word : text) {
				context.write(new Text(word), new Text(key.get() + ""));
			}
		}
	}

}
