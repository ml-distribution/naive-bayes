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
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 计算每个类别下的每个词的概率
 * 
 * @author wgybzb
 *
 */
public class ClassifyMapper extends Mapper<Text, Text, LongWritable, Text> {

	private long wordsSize;
	private HashMap<String, Integer> wordsUnderCate;

	@Override
	protected void setup(Context context) throws IOException {
		wordsSize = context.getConfiguration().getLong(NavieBayesDistribute.UNIQUE_WORDS, 100);
		wordsUnderCate = new HashMap<String, Integer>();

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
				wordsUnderCate.put(cate, new Integer(Integer.parseInt(counts[1])));
			}
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {

		String[] elements = value.toString().split("::");
		String model = elements[0];

		// 模型的分类及其次数
		HashMap<String, Integer> modelCounts = null;
		if (model.length() > 0) {
			String[] cateCounts = model.split(" ");
			modelCounts = new HashMap<String, Integer>();
			for (String cateCount : cateCounts) {
				String[] elems = cateCount.split(":");
				modelCounts.put(elems[0], new Integer(Integer.parseInt(elems[1])));
			}
		}

		// 该词语在每个文档中出现的次数
		HashMap<Long, Integer> multipliers = new HashMap<Long, Integer>();
		HashMap<Long, String> trueLabels = new HashMap<Long, String>();
		for (int i = 1; i < elements.length; ++i) {
			String[] elems = elements[i].split(",");
			Long docId = new Long(Long.parseLong(elems[0]));
			multipliers.put(docId, new Integer(multipliers.containsKey(docId) ? multipliers.get(docId).intValue() + 1
					: 1));
			if (elems.length > 1) {
				if (!trueLabels.containsKey(docId)) {
					// 添加该文档的真实类别列表
					// 假设: 相同的文档有相同的类别
					StringBuilder list = new StringBuilder();
					for (int j = 1; j < elems.length; ++j) {
						list.append(String.format("%s:", elems[j]));
					}
					String outval = list.toString();
					trueLabels.put(docId, outval.substring(0, outval.length() - 1));
				}
			}
		}

		// 循环每个类别, 对于每个文档ID，计算词语在类别下面的概率
		for (Long docId : trueLabels.keySet()) {
			StringBuilder probs = new StringBuilder();
			for (String label : wordsUnderCate.keySet()) {
				int wordLabelCount = wordsUnderCate.get(label).intValue();
				int count = 0;
				if (modelCounts != null && modelCounts.containsKey(label)) {
					count = modelCounts.get(label).intValue();
				}

				int multiplier = multipliers.get(docId);
				double wordProb = multiplier
						* (Math.log(count + NavieBayesDistribute.ALPHA) - Math.log(wordLabelCount
								+ (NavieBayesDistribute.ALPHA * wordsSize)));
				probs.append(String.format("%s:%s,", label, wordProb));
			}
			// 输出文档ID——><cate:wordProbability>列表::真实类别
			String output = probs.toString();
			context.write(new LongWritable(docId),
					new Text(String.format("%s::%s", output.substring(0, output.length() - 1), trueLabels.get(docId))));
		}
	}

}
