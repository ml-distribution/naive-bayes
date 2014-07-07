package zx.soft.naive.bayes.forecast;

import java.util.HashMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

/**
 * 分类预测主类
 * 
 * @author wanggang
 *
 */
public class ForecastCore {

	private static Logger logger = LoggerFactory.getLogger(ForecastCore.class);

	private final TrainModel trainModel;

	private final AnalyzerTool analyzerTool;

	public ForecastCore() {
		logger.info("加载训练模型数据......");
		trainModel = new TrainModel();
		logger.info("加载分词器......");
		analyzerTool = new AnalyzerTool();
	}

	/**
	 * 测试函数
	 */
	public static void main(String[] args) {

		ForecastCore forecastCore = new ForecastCore();
		System.out.println(forecastCore.classify("悲伤的一天"));
		forecastCore.close();

	}

	/**
	 * 对文本进行分类
	 */
	public String classify(String text) {
		HashMap<String, Integer> wordAndCounts = analyzerTool.getWordAndCounts(text);
		double bestProb = Double.NEGATIVE_INFINITY;
		String bestCate = null;
		double totalProb;
		for (String cate : trainModel.getCates()) {
			totalProb = trainModel.getCatePirorProb(cate);
			for (Entry<String, Integer> wordAndCount : wordAndCounts.entrySet()) {
				totalProb += wordAndCount.getValue() * trainModel.getWordInCateProb(wordAndCount.getKey(), cate);
			}
			if (totalProb > bestProb) {
				bestCate = cate;
				bestProb = totalProb;
			}
		}
		return bestCate;
	}

	public void close() {
		analyzerTool.close();
	}

}
