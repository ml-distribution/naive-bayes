package zx.soft.naive.bayes.forecast;

import java.util.HashMap;
import java.util.List;

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
		trainModel = new TrainModel();
		analyzerTool = new AnalyzerTool();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	private HashMap<String, Integer> getWordAndCounts(String text) {
		HashMap<String, Integer> result = new HashMap<>();
		List<String> words = analyzerTool.analyzerTextToList(text);
		for (String word : words) {
			if (result.get(word) == null) {
				result.put(word, 1);
			} else {
				result.put(word, result.get(word) + 1);
			}
		}
		return result;
	}

	public void close() {
		analyzerTool.close();
	}

}
