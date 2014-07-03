package zx.soft.navie.bayes.mapred.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算每个文档的累加的log(prob(word|cate))概率和
 * 
 * @author wgybzb
 *
 */
public class ForecastReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	private HashMap<String, Double> priorOfCates;

	@Override
	protected void setup(Context context) throws IOException {
		priorOfCates = new HashMap<>();

		// 在DistributedCache中建立HashMap存放类别出现的概率
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
				priorOfCates.put(elems[0], Double.parseDouble(elems[1]));
			}
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws InterruptedException,
			IOException {

		// 记录docId在每个类别下面的概率
		HashMap<String, Double> probsOfDocInCate = new HashMap<>();

		// 每次循环的value是一个“词语对应类别概率列表”格式
		for (Text value : values) {
			String[] wordProbOfCates = value.toString().split(",");
			for (String wordProbOfCate : wordProbOfCates) {
				String[] pieces = wordProbOfCate.split(":");
				String cate = pieces[0];
				double prob = Double.parseDouble(pieces[1]);
				probsOfDocInCate.put(cate, probsOfDocInCate.containsKey(cate) ? probsOfDocInCate.get(cate) + prob
						: prob);
			}
		}

		// 循环每个类别, 增加先验概率，并计算最相近的类别 
		double bestProb = Double.NEGATIVE_INFINITY;
		String bestCate = null;
		for (String cate : probsOfDocInCate.keySet()) {
			double totalProb = probsOfDocInCate.get(cate) + priorOfCates.get(cate);
			if (totalProb > bestProb) {
				bestCate = cate;
				bestProb = totalProb;
			}
		}

		// 输出:docId——>cate
		context.write(key, new Text(bestCate));
	}

}
