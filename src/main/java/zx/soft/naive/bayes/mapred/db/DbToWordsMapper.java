package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

public class DbToWordsMapper extends Mapper<LongWritable, DbInputWritable, Text, LongWritable> {
	private static final AnalyzerTool analyzerTool = new AnalyzerTool();

	@Override
	protected void map(LongWritable key, DbInputWritable value, Context context) throws IOException,
			InterruptedException {
		List<String> wordList = new ArrayList<>();
		wordList = analyzerTool.analyzerTextToList(value.getText());
		for (String word : wordList) {
			context.write(new Text(word), new LongWritable(1));
		}
	}

}
