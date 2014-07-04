package zx.soft.naive.bayes.mapred.core;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算每个文档中的每个词语在所有类别下面的概率。
 * 
 * @author wgybzb
 *
 */
public class ForecastMapper extends Mapper<Text, Text, LongWritable, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {

		String[] elements = value.toString().split("::");
		/*
		 * 对于当前词语不在训练模型中的情况，不做处理
		 */
		if (elements[0].length() == 0) {
			return;
		}

		// 该词语（key）在每个类别中出现的概率：cate1:0.0003 cate2:0.00004::docId-i::docId-j
		String wordProbOfCates = elements[0];

		// 该词语的cate:prob对
		HashMap<String, Double> wordProbInCates = null;
		if (wordProbOfCates.length() > 0) {
			String[] cateProbs = wordProbOfCates.split(",");
			wordProbInCates = new HashMap<>();
			for (String cateCount : cateProbs) {
				String[] elems = cateCount.split(":");
				wordProbInCates.put(elems[0], Double.parseDouble(elems[1]));
			}
		}

		// 记录该词语在每个文档中出现的次数,docId:count
		HashMap<Long, Integer> docIdCount = new HashMap<>();
		// 循环出现该词语的每个文档
		for (int i = 1; i < elements.length; ++i) {
			// 文档Id
			long docId = Long.parseLong(elements[i]);
			docIdCount.put(docId, docIdCount.containsKey(docId) ? docIdCount.get(docId).intValue() + 1 : 1);
		}

		// 循环出现该词语的每个文档，计算每个文档中该词语在每个类别下出现的概率，增加了权重系数
		for (long docId : docIdCount.keySet()) {
			StringBuilder probs = new StringBuilder();
			// 计算该词语在所有类别下分别出现的概率
			for (String cate : wordProbInCates.keySet()) {
				// 该词语在文档docId中出现的次数,可认为是权重
				int weight = docIdCount.get(docId);
				//   word在docId中出现的次数×log(word在cate下出现的次数/(cate的词语总数+词语总数))
				// = 权重×word在cate下出现的概率	
				double wordProbOfCate = weight * wordProbInCates.get(cate);
				// 该词语在cate下出现的概率
				probs.append(String.format("%s:%s,", cate, wordProbOfCate));
			}
			// 输出文档ID——><cate:wordProbOfCate>列表
			context.write(new LongWritable(docId), new Text(probs.toString()
					.substring(0, probs.toString().length() - 1)));
		}
	}

}
