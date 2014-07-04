package zx.soft.naive.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

public class DbToHdfsReducer extends Reducer<LongWritable, DbInputWritable, LongWritable, Text> {

	private static final AnalyzerTool analyzerTool = new AnalyzerTool();

	@Override
	protected void reduce(LongWritable key, Iterable<DbInputWritable> values, Context context) throws IOException,
			InterruptedException {

		for (DbInputWritable value : values) {
			context.write(key, new Text(value.getWid() + " " + analyzerTool.analyzerTextToStr(value.getText(), " ")));
		}

	}

}
