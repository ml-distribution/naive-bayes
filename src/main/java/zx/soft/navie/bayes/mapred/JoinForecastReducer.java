package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 在Reduce阶段，将训练模型和测试预测数据结合。
 * 1）构建好的训练模型数据
 *    word——>cate1:3 cate2:4::docID1,cat21,cate2::docID2,cate3,cate4
 * 2）包含"文档ID,类别列表"的测试数据
 *    word——>::docID1,cate1,cate2::docID2,cate3,cate4
 * 3) 包含"文档ID"的预测数据
 *    word——>::docID1::docID2
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
				// 构建好的训练模型数据
				modelLine = line;
			} else {
				// 包含"文档ID,类别列表"的预测数据
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
