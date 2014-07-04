package zx.soft.naive.bayes.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 读取输入文件数据，输出格式为：word——>cate
 * 
 * @author wgybzb
 *
 */
public class TrainWordMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

		String[] words = value.toString().split("\\s+");
		Vector<String> cates = TrainsVector.tokenizeCates(words[0]);
		Vector<String> text = TrainsVector.tokenizeDoc(words);

		for (String cate : cates) {
			for (String word : text) {
				// (C = c, W = w)
				context.write(new Text(word), new Text(cate));
			}
		}
	}

}
