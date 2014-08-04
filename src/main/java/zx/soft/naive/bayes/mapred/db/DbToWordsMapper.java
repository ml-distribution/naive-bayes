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

	@Override
	protected void map(LongWritable key, DbInputWritable value, Context context) throws IOException,
			InterruptedException {
		wordList = analyzerTool.analyzerTextToList(value.getText());
		for (String word : wordList) {
			//			去掉指定格式的分词，如以abc,123,_中文开头的词语等 
			//			去空格，中间去数字字母
			//			--------------
			if (!pattern.matcher(word).find()) {
				// 去除单个词的统计
				if (word.length() > 1) {
					context.write(new Text(word), ONE);
				}
			}
		}
	}

}
