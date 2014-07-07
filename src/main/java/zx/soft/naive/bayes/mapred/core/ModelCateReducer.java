package zx.soft.naive.bayes.mapred.core;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zx.soft.naive.bayes.mapred.NaiveBayesConstant;

/**
 * 计算每个类别出现的先验概率
 * 
 * @author wanggang
 *
 */
public class ModelCateReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	private long totalSamples;
	private long uniqueCates;

	@Override
	protected void setup(Context context) throws IOException {
		totalSamples = context.getConfiguration().getLong(NaiveBayesConstant.TOTAL_SAMPLES, 100);
		uniqueCates = context.getConfiguration().getLong(NaiveBayesConstant.UNIQUE_CATES, 100);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

		for (Text value : values) {
			String[] elems = value.toString().split(":");
			// 该类别下的文档总数
			int docsCount = Integer.parseInt(elems[0]);
			// cate下的文档总数/(样本总数+类别总数)，可以在训练模型中计算出来
			double prior = Math.log(docsCount + NaiveBayesConstant.ALPHA)
					- Math.log(totalSamples + (NaiveBayesConstant.ALPHA * uniqueCates));
			context.write(key, new DoubleWritable(prior));
		}
	}

}
