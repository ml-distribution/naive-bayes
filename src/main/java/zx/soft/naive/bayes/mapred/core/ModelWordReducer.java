package zx.soft.naive.bayes.mapred.core;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 计算每个词语在所有类别下面的概率。
 * 
 * 输出：word——>cate-i:0.0005，cate-j:0.00009，cate-k:0.000041
 * 
 * @author wanggang
 *
 */
public class ModelWordReducer extends Reducer<Text, Text, Text, Text> {

	private long wordsSize;
	private HashMap<String, Integer> wordsCountPerCate;

	@Override
	protected void setup(Context context) throws IOException {
		// 总的词语量
		wordsSize = context.getConfiguration().getLong(NaiveBayesConstant.UNIQUE_WORDS, 100);
		// 每个类别下的词语总量
		wordsCountPerCate = new HashMap<>();

		// 在DistributedCache下建立一个类别数据的HashMap
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length < 1) {
			throw new IOException("DistributedCache returned an empty file set!");
		}

		// 从DistributedCache中读取分片数据
		LocalFileSystem lfs = FileSystem.getLocal(context.getConfiguration());
		for (Path file : files) {
			FSDataInputStream input = lfs.open(file);
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String[] elems = line.split("\\s+");
				String cate = elems[0];
				String[] counts = elems[1].split(":");
				wordsCountPerCate.put(cate, Integer.parseInt(counts[1]));
			}
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		for (Text value : values) {
			// 该词语（key）在每个类别中出现的次数：“cate-i:10 cate-j:9 cate-k:41”
			String wordsInfo = value.toString();

			// 该词语的cate：count对
			HashMap<String, Integer> cateCountsOfWord = null;
			if (wordsInfo.length() > 0) {
				String[] cateCounts = wordsInfo.split(" ");
				cateCountsOfWord = new HashMap<>();
				for (String cateCount : cateCounts) {
					String[] elems = cateCount.split(":");
					cateCountsOfWord.put(elems[0], Integer.parseInt(elems[1]));
				}
			}

			// 计算该词语在所有类别下分别出现的概率
			StringBuilder probs = new StringBuilder();
			for (String cate : wordsCountPerCate.keySet()) {
				// 类别cate下的词语总数
				int wordCountOfCate = wordsCountPerCate.get(cate).intValue();
				// 该词语在cate下出现的次数
				int count = 0;
				if (cateCountsOfWord != null && cateCountsOfWord.containsKey(cate)) {
					count = cateCountsOfWord.get(cate).intValue();
				}
				// word在cate下出现的概率 = log(word在cate下出现的次数/(cate的词语总数+词语总数))	
				double wordProbOfCate = (Math.log(count + NaiveBayesConstant.ALPHA) - Math.log(wordCountOfCate
						+ (NaiveBayesConstant.ALPHA * wordsSize)));
				// 该词语在cate下出现的概率
				probs.append(String.format("%s:%s,", cate, wordProbOfCate));
			}
			// word——><cate:wordProbOfCate>列表
			context.write(key, new Text(probs.toString().substring(0, probs.toString().length() - 1)));
		}
	}

}
