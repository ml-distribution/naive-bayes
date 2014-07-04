package zx.soft.navie.bayes.mapred.db;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

public class DbToHdfsReducer extends Reducer<LongWritable, DbInputWritable, NullWritable, Text> {

	private static final AnalyzerTool analyzerTool = new AnalyzerTool();

	@Override
	protected void reduce(LongWritable key, Iterable<DbInputWritable> values, Context context) throws IOException,
			InterruptedException {

		for (DbInputWritable value : values) {
			context.write(NullWritable.get(),
					new Text(value.getWid() + " " + analyzerTool.analyzerTextToStr(value.getText(), " ")));
		}

	}

}
