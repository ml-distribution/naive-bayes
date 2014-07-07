package zx.soft.naive.bayes.forecast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 训练模型数据类
 * 
 * @author wanggang
 *
 */
public class TrainModel {

	private static Logger logger = LoggerFactory.getLogger(TrainModel.class);

	private static final String MODEL_CATES = "train-model/modelCates/";

	private static final String MODEL_WORDS = "train-model/modelWords/";

	private static HashMap<String, Double> catePirorProbs;

	private static HashMap<String, HashMap<String, Double>> wordInCateProbs;

	public TrainModel() {
		// 初始化cate出现的概率
		initCatePirorProbs();
		// 初始化word在cate下出现的概率
		initWordInCateProbs();
	}

	/**
	 * 获取某个类别的先验概率
	 */
	public double getCatePirorProb(String cate) {
		return catePirorProbs.get(cate);
	}

	/**
	 * 初始化cate出现的概率
	 */
	private void initCatePirorProbs() {
		catePirorProbs = new HashMap<>();
		String str;
		String[] elems;
		try (BufferedReader br = new BufferedReader(new FileReader(new File(MODEL_CATES + "part-r-00000")));) {
			while ((str = br.readLine()) != null) {
				elems = str.split("\\s+");
				catePirorProbs.put(elems[0], Double.parseDouble(elems[1]));
			}
		} catch (IOException e) {
			logger.error("IOException:" + e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * word在cate下出现的概率
	 */
	public double getWordInCateProb(String word, String cate) {
		return wordInCateProbs.get(word).get(cate);
	}

	/**
	 * 初始化word在cate下出现的概率
	 */
	private void initWordInCateProbs() {
		wordInCateProbs = new HashMap<>();
		try {
			for (int i = 0; i < 20; i++) {
				String uri = MODEL_WORDS + "part-r-000" + i;
				if (i < 10) {
					uri = MODEL_WORDS + "part-r-0000" + i;
				}
				Configuration conf = new Configuration();
				SequenceFile.Reader reader = null;
				String[] elems;
				try {
					reader = new SequenceFile.Reader(FileSystem.get(URI.create(uri), conf), new Path(uri), conf);
					Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
					Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
					while (reader.next(key, value)) {
						HashMap<String, Double> cateProbs = new HashMap<>();
						for (String cateProb : value.toString().split(",")) {
							elems = cateProb.split(":");
							cateProbs.put(elems[0], Double.parseDouble(elems[1]));
						}
						wordInCateProbs.put(key.toString(), cateProbs);
					}
				} finally {
					IOUtils.closeStream(reader);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
