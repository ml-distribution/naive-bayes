package zx.soft.naive.bayes.mapred.txt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

/**
 * 输出格式如下: catei wordi wordj wordk
 * 
 * @author wanggang
 *
 */

public class TxtToHdfsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private static final AnalyzerTool analyzerTool = new AnalyzerTool();
	private static FileSplit fileSplit;
	private static String fileName;
	private static String cate;
	private static String reval;

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

		if (value.toString().length() > 0) {
			fileSplit = (FileSplit) context.getInputSplit();
			fileName = fileSplit.getPath().toUri().toString();
			cate = fileName.substring(fileName.lastIndexOf("/") + 1);
			reval = analyzerTool.analyzerTextToStr(value.toString(), " ");
			context.write(key, new Text(cate + " " + reval));
		}

	}

}
