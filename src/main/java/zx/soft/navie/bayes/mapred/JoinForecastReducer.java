package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 在Reduce阶段，将训练模型和预测数据结合.
 * 
 * @author wanggang
 *
 */
public class JoinForecastReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

		String modelLine = null;
		ArrayList<String> documents = new ArrayList<String>();
		for (Text value : values) {
			String line = value.toString();
			if (line.contains(":")) {
				// 构建好的模型数据
				modelLine = line;
			} else {
				// 包含"文档ID,类别列表"的测试数据
				documents.add(line);
			}
		}

		if (documents.size() > 0) {
			if (modelLine == null) {
				modelLine = "";
			}
			StringBuilder output = new StringBuilder();
			output.append(String.format("%s::", modelLine));
			for (String doc : documents) {
				output.append(String.format("%s::", doc));
			}
			String out = output.toString();
			context.write(key, new Text(out.substring(0, out.length() - 2)));
		}
	}

}
