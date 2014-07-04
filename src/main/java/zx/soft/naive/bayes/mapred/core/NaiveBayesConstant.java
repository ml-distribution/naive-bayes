package zx.soft.naive.bayes.mapred.core;

/**
 * Navie Bayes用到的常量
 * 
 * @author wanggang
 *
 */
public class NaiveBayesConstant {

	public static final double ALPHA = 1.0;

	static enum NB_COUNTERS {
		TOTAL_SAMPLES, UNIQUE_WORDS, UNIQUE_CATES
	}

	// 样本数，如果每个文档是单个类别的话，文档数和样本数一样，否则样本数大于文档数，实际则以样本数为准
	public static final String TOTAL_SAMPLES = "naive.bayes.total_samples";
	// 词数
	public static final String UNIQUE_WORDS = "naive.bayes.unique_words";
	// 类别数
	public static final String UNIQUE_CATES = "naive.bayes.unique_cates";

}
