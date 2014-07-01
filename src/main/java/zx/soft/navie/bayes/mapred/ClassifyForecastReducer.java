package zx.soft.navie.bayes.mapred;

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
public class ClassifyForecastReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	private long totalSamples;
	private long uniqueCates;
	private HashMap<String, Integer> docsWithCate;

	@Override
	protected void setup(Context context) throws IOException {
		totalSamples = context.getConfiguration().getLong(NavieBayesDistribute.TOTAL_SAMPLES, 100);
		uniqueCates = context.getConfiguration().getLong(NavieBayesDistribute.UNIQUE_LABELS, 100);
		docsWithCate = new HashMap<String, Integer>();

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
				String label = elems[0];
				String[] counts = elems[1].split(":");
				docsWithCate.put(label, new Integer(Integer.parseInt(counts[0])));
			}
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws InterruptedException,
			IOException {

		HashMap<String, Double> probabilities = new HashMap<String, Double>();

		for (Text value : values) {
			// 每次循环的value是一个“词语对应类别概率列表”格式
			String[] elements = value.toString().split("::");
			String[] labelProbs = elements[0].split(",");
			for (String labelProb : labelProbs) {
				String[] pieces = labelProb.split(":");
				String label = pieces[0];
				double prob = Double.parseDouble(pieces[1]);
				probabilities.put(label, new Double(probabilities.containsKey(label) ? probabilities.get(label)
						.doubleValue() + prob : prob));
			}
		}

		// 循环每个类别, 增加先验概率，并计算最相近的类别 
		double bestProb = Double.NEGATIVE_INFINITY;
		String bestLabel = null;
		for (String label : probabilities.keySet()) {
			double prior = Math.log(docsWithCate.get(label).intValue() + NavieBayesDistribute.ALPHA)
					- Math.log(totalSamples + (NavieBayesDistribute.ALPHA * uniqueCates));
			double totalProb = probabilities.get(label).doubleValue() + prior;
			if (totalProb > bestProb) {
				bestLabel = label;
				bestProb = totalProb;
			}
		}

		// 输出：docID——>cate
		context.write(key, new Text(bestLabel));
	}

}
