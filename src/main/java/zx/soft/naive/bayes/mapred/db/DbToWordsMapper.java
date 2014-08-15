package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

public class DbToWordsMapper extends Mapper<LongWritable, DbInputWritable, Text, IntWritable> {

	private static final AnalyzerTool analyzerTool = new AnalyzerTool();

	private static List<String> wordList = null;
	private static Pattern pattern = Pattern.compile("^[0-9a-zA-Z_]");

	private static final IntWritable ONE = new IntWritable(1);
	private static boolean isAdvertisementTag = false;

	@Override
	protected void map(LongWritable key, DbInputWritable value, Context context) throws IOException,
			InterruptedException {
		/**
		 * 对广告相关微博语料进行分词
		 */
		// 依据广告关键字匹配，只对内容是广告的微博进行分词
		String[] containAdvertisingKeywords = { "微博客户端", "推荐一个", "美图秀秀单纯考试", "促销", "秒杀", "限量", "减肥药", "打折", "特价", "包邮",
				"淘宝", "天猫", "折扣", "限时", "数量有限", "活动商品", "欢迎预订", "抢购", "免费使用", "代购" };
		String[] isAdvertisingKeywords = { "转发微博", "分享图片", "美图秀秀Andriod版", "美图秀秀iPhone版" };
		//先判断微博内容是否等于广告关键字
		for (String keyword1 : isAdvertisingKeywords) {
			if (value.getText().equals(keyword1)) {
				wordList = analyzerTool.analyzerTextToList(value.getText());
				isAdvertisementTag = true;
				break;
			}
		}
		if (!isAdvertisementTag) {
			//判断微博内容是否包含广告关键字
			for (String keyword2 : containAdvertisingKeywords) {
				if (value.getText().contains(keyword2)) {
					wordList = analyzerTool.analyzerTextToList(value.getText());
					isAdvertisementTag = true;
					break;
				}
			}
		}

		if (isAdvertisementTag) {
			//		wordList = analyzerTool.analyzerTextToList(value.getText());
			for (String word : wordList) {
				//			去掉指定格式的分词，如以abc,123,_开头的词语
				if (!pattern.matcher(word).find()) {
					// 去除单个词的统计
					if (word.length() > 1) {
						context.write(new Text(word), ONE);
					}
				}
			}
		}
		/**
		 * 对所有微博语料直接分词
		 */
		//		wordList = analyzerTool.analyzerTextToList(value.getText());
		//		for (String word : wordList) {
		//			//			去掉指定格式的分词，如以abc,123,_开头的词语
		//			if (!pattern.matcher(word).find()) {
		//				// 去除单个词的统计
		//				if (word.length() > 1) {
		//					context.write(new Text(word), ONE);
		//				}
		//			}
		//		}
	}
}
