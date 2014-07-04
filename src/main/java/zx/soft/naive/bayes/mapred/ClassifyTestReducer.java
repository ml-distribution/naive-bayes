package zx.soft.naive.bayes.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zx.soft.naive.bayes.mapred.core.NaiveBayesConstant;

/**
 * 计算每个文档的累加的log(prob(word|cate))概率和
 * 
 * @author wgybzb
 *
 */
public class ClassifyTestReducer extends Reducer<LongWritable, Text, LongWritable, IntWritable> {

	private long totalSamples;
	private long uniqueCates;
	private HashMap<String, Integer> docsCountPerCate;

	@Override
	protected void setup(Context context) throws IOException {
		totalSamples = context.getConfiguration().getLong(NaiveBayesConstant.TOTAL_SAMPLES, 100);
		uniqueCates = context.getConfiguration().getLong(NaiveBayesConstant.UNIQUE_CATES, 100);
		docsCountPerCate = new HashMap<>();

		// 在DistributedCache中建立HashMap存放类别数据
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length < 1) {
			throw new IOException("DistributedCache returned an empty file set!");
		}

		// 从DistributedCache中读取数据
		LocalFileSystem lfs = FileSystem.getLocal(context.getConfiguration());
		for (Path file : files) {
			FSDataInputStream input = lfs.open(file);
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String[] elems = line.split("\\s+");
				String cate = elems[0];
				String[] counts = elems[1].split(":");
				docsCountPerCate.put(cate, Integer.parseInt(counts[0]));
			}
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws InterruptedException,
			IOException {

		// 记录docId在每个类别下面的概率
		HashMap<String, Double> probsOfDocInCate = new HashMap<>();
		ArrayList<String> trueCates = null;

		// 每次循环的value是一个“词语对应类别概率列表”格式
		for (Text value : values) {
			String[] elements = value.toString().split("::");
			String[] wordProbOfCates = elements[0].split(",");
			for (String wordProbOfCate : wordProbOfCates) {
				String[] pieces = wordProbOfCate.split(":");
				String cate = pieces[0];
				double prob = Double.parseDouble(pieces[1]);
				probsOfDocInCate.put(cate, probsOfDocInCate.containsKey(cate) ? probsOfDocInCate.get(cate)
						.doubleValue() + prob : prob);
			}

			// 同时也需要真实类别
			if (trueCates == null) {
				String[] list = elements[1].split(":");
				trueCates = new ArrayList<>();
				for (String elem : list) {
					trueCates.add(elem);
				}
			}
		}

		// 循环每个类别, 增加先验概率，并计算最相近的类别 
		double bestProb = Double.NEGATIVE_INFINITY;
		String bestCate = null;
		for (String cate : probsOfDocInCate.keySet()) {
			// cate下的文档总数/(样本总数+类别总数)，可以在训练模型中计算出来
			double prior = Math.log(docsCountPerCate.get(cate).intValue() + NaiveBayesDistribute.ALPHA)
					- Math.log(totalSamples + (NaiveBayesDistribute.ALPHA * uniqueCates));
			double totalProb = probsOfDocInCate.get(cate).doubleValue() + prior;
			if (totalProb > bestProb) {
				bestCate = cate;
				bestProb = totalProb;
			}
		}

		// 输出正确与否
		context.write(key, new IntWritable(trueCates.contains(bestCate) ? 1 : 0));
	}

}
