package zx.soft.naive.bayes.mapred;

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
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算每个类别下的每个词的概率
 * 
 * @author wgybzb
 *
 */
public class ClassifyMapper extends Mapper<Text, Text, LongWritable, Text> {

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
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {

		String[] elements = value.toString().split("::");
		// 该词语（key）在每个类别中出现的次数：“cate-i:10 cate-j:9 cate-k:41”
		String wordsInfo = elements[0];

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

		// 记录该词语在每个文档中出现的次数,docId:count
		HashMap<Long, Integer> docIdCount = new HashMap<>();
		// 记录真实类别,docId:cate-i:cate-j
		HashMap<Long, String> trueCates = new HashMap<>();
		// 循环出现该词语的每个文档
		for (int i = 1; i < elements.length; ++i) {
			// docId,cate
			String[] elems = elements[i].split(",");
			// 文档Id
			long docId = Long.parseLong(elems[0]);
			docIdCount.put(docId, docIdCount.containsKey(docId) ? docIdCount.get(docId).intValue() + 1 : 1);
			// 存在多个类别的情况下
			if (elems.length > 1) {
				if (!trueCates.containsKey(docId)) {
					// 添加该文档的真实类别列表
					// 假设: 相同的文档有相同的类别
					StringBuilder cateList = new StringBuilder();
					for (int j = 1; j < elems.length; ++j) {
						cateList.append(String.format("%s:", elems[j]));
					}
					trueCates.put(docId, cateList.toString().substring(0, cateList.toString().length() - 1));
				}
			}
		}

		// 循环出现该词语的每个文档，计算每个文档中该词语在每个类别下出现的概率，增加了权重系数
		for (long docId : trueCates.keySet()) {
			StringBuilder probs = new StringBuilder();
			// 计算该词语在所有类别下分别出现的概率
			for (String cate : wordsCountPerCate.keySet()) {
				// 类别cate下的词语总数
				int wordCountOfCate = wordsCountPerCate.get(cate).intValue();
				// 该词语在cate下出现的次数
				int count = 0;
				if (cateCountsOfWord != null && cateCountsOfWord.containsKey(cate)) {
					count = cateCountsOfWord.get(cate).intValue();
				}
				// 该词语在文档docId中出现的次数,可认为是权重
				int weight = docIdCount.get(docId);
				//   word在docId中出现的次数×log(word在cate下出现的次数/(cate的词语总数+词语总数))
				// = 权重×word在cate下出现的概率	
				double wordProbOfCate = weight
						* (Math.log(count + NaiveBayesConstant.ALPHA) - Math.log(wordCountOfCate
								+ (NaiveBayesConstant.ALPHA * wordsSize)));
				// 该词语在cate下出现的概率
				probs.append(String.format("%s:%s,", cate, wordProbOfCate));
			}
			// 输出文档ID——><cate:wordProbOfCate>列表::真实类别
			context.write(
					new LongWritable(docId),
					new Text(String.format("%s::%s", probs.toString().substring(0, probs.toString().length() - 1),
							trueCates.get(docId))));
		}
	}

}
