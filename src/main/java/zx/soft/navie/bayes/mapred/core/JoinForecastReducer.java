package zx.soft.navie.bayes.mapred.core;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 在Reduce阶段，将训练模型和预测数据结合。
 * 
 * 输出格式：
 *    word——>cate1:0.0003 cate2:0.00004::docId-i::docId-j
 *    
 * @author wanggang
 *
 */
public class JoinForecastReducer extends Reducer<Text, Text, Text, Text> {

	private static final int LIMIT_LENGTH = 2000;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

		String modelLine = null;
		ArrayList<String> docIds = new ArrayList<>();
		for (Text value : values) {
			String line = value.toString();
			if (line.contains(":")) {
				// 构建好的训练模型数据
				modelLine = line;
			} else {
				// 包含"文档ID"的预测数据
				docIds.add(line);
			}
		}

		// 有可能存在，某些词语不在训练模型中，赋值为空或者丢弃
		if (modelLine == null) {
			//			modelLine = "";
			return;
		}
		StringBuilder output = null;
		if (docIds.size() > 0) {
			output = new StringBuilder();
			output.append(String.format("%s::", modelLine));
			for (String doc : docIds) {
				output.append(String.format("%s::", doc));
				// 防止内存泄漏
				if (output.toString().length() > LIMIT_LENGTH) {
					context.write(key, new Text(output.toString().substring(0, output.toString().length() - 2)));
					output = new StringBuilder();
					output.append(String.format("%s::", modelLine));
				}
			}
			context.write(key, new Text(output.toString().substring(0, output.toString().length() - 2)));
		}
	}

}
