package zx.soft.navie.bayes.simple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.navie.bayes.analyzer.AnalyzerTool;
import zx.soft.navie.bayes.data.TextTrainDataFactory;
import zx.soft.navie.bayes.data.TrainDataFactory;

/**
 * Navie Bayes单节点实现
 * @author zhumm
 *
 */
public class NavieBayesSimple {

	private static Logger logger = LoggerFactory.getLogger(NavieBayesSimple.class);

	private final AnalyzerTool analyzerTool;
	private final TrainDataFactory trainDataFactory;

	public NavieBayesSimple(TrainDataFactory trainDataFactory) {
		this.analyzerTool = new AnalyzerTool();
		this.trainDataFactory = trainDataFactory;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			logger.error("Usage: <transDir> <text>");
			System.exit(-1);
		}
		NavieBayesSimple bayes = new NavieBayesSimple(new TextTrainDataFactory(args[0]));
		System.out.println(bayes.classifyText(args[1]));
	}

	/**
	 * 对文本进行分类
	 * @param text
	 * @return
	 */
	public String classifyText(String text) {
		Map<Double, String> result = new HashMap<>();
		double probility = 0f;
		logger.info("Calcute text's posteriori probability in every cate's condition ...");
		for (String cate : trainDataFactory.getCates()) {
			probility = calculatePosterioriProbability(text, cate);
			result.put(probility, cate);
			logger.info("Text in " + cate + "'s probility = " + probility);
		}
		// 排序找到相似度最高的  
		Double max = -1d;
		for (Entry<Double, String> temp : result.entrySet()) {
			max = Math.max(max, temp.getKey());
		}

		return result.get(max);
	}

	/**
	 * <br> 
	 *    P(xi∣cj)=P(cjxi)/P(cj)
	 * </br>
	 * @param cate: 当前类别
	 * @param word: 某个词
	 * @return: 某个词在当前类别中出现的概率
	 */
	public double calcuteProbabilityOfWordInCate(String cate, String word) {
		// 当前分类中包含当前关键字的训练文本的数目
		double numOfSampleIntCateContainWord = numOfSampleIntCateContainWord(cate, word);
		// 当前类别下的样本数量
		double numOfSampleInCate = trainDataFactory.numOfSampleInCate(cate);
		// laplace平滑
		double probability = (numOfSampleIntCateContainWord + 1) / (numOfSampleInCate + 1);
		return probability;
	}

	/**
	 * 当前类别中包含关键字 aKey 的训练样本数目
	 * @param cate: 当前类别
	 * @param word: 某个词
	 * @return： 样本数
	 */
	public int numOfSampleIntCateContainWord(String cate, String word) {
		int count = 0;
		String[] pathset = trainDataFactory.getPathSet(cate);
		for (int j = 0; j < pathset.length; j++) {
			String text = trainDataFactory.getSampleContent(pathset[j]);
			if (text.contains(word)) {
				count++;
			}
		}
		return count;
	}

	/**
	 * <br>
	 *     当前类别与输入文本的相似度
	 * </br>
	 * @param text
	 * @param cate
	 * @return
	 */
	public double calculatePosterioriProbability(String text, String cate) {
		List<String> words = analyzerTool.analyzerTextToList(text);
		double probability = 1.0;
		// 条件概率连乘
		for (String word : words) {
			probability *= calcuteProbabilityOfWordInCate(cate, word);
		}
		// 当前分类的训练文本数目
		double numOfSampleInCate = trainDataFactory.numOfSampleInCate(cate);
		// 全部文本数目
		double totalNumOfSample = trainDataFactory.totalNumOfSample();
		// 再乘以先验概率(全部文本数目比上当前类别的文本数目)
		probability *= numOfSampleInCate / totalNumOfSample;
		return probability;
	}

}