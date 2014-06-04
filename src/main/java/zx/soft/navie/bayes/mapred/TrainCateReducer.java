package zx.soft.navie.bayes.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  统计每个类别下面的文档数和词汇量，输出格式：
 *   cate——> cate下的文档数:cate下的词汇量
 *  此后，可以将输出序列化到DistributedCache中，以便分类器获取数据。
 * @author wgybzb
 */
public class TrainCateReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException,
			IOException {

		// 统计类别个数
		context.getCounter(NavieBayesDistribute.NB_COUNTERS.UNIQUE_LABELS).increment(1);

		long pY = 0;
		long pYW = 0;
		for (IntWritable value : values) {
			// 统计全部的文档数，也就是样本数
			context.getCounter(NavieBayesDistribute.NB_COUNTERS.TOTAL_DOCS).increment(1);
			// 统计类别为key的文档数
			pY++;
			// 统计类别为key的所有文档下的词量总数
			pYW += value.get();
		}

		context.write(key, new Text(String.format("%s:%s", pY, pYW)));
	}

}
