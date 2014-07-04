package zx.soft.naive.bayes.mapred.core;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 统计每个词对应类别及其出现次数，输出格式word——>catei:n1 catej:n2 catek:n3 ...
 * 
 * @author wgybzb
 *
 */
public class TrainWordReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

		// 更新词汇量计数器
		context.getCounter(NaiveBayesConstant.NB_COUNTERS.UNIQUE_WORDS).increment(1);

		// 统计各个类别及其出现次数
		HashMap<String, Integer> counts = new HashMap<>();
		for (Text cate : values) {
			String cateKey = cate.toString();
			counts.put(cateKey, counts.containsKey(cateKey) ? counts.get(cateKey).intValue() + 1 : 1);
		}
		StringBuilder outKey = new StringBuilder();
		for (String cate : counts.keySet()) {
			outKey.append(String.format("%s:%s ", cate, counts.get(cate).intValue()));
		}

		// 输出word——>catei:n1 catej:n2 catek:n3 ...
		context.write(key, new Text(outKey.toString().trim()));
	}

}
