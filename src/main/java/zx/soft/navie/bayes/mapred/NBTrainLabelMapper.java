package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 *
 * Handles parsing the input documents and sorting out label statistics.
 */
public class NBTrainLabelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String[] words = value.toString().split("\\s+");
		Vector<String> labels = NBController.tokenizeLabels(words[0]);
		Vector<String> text = NBController.tokenizeDoc(words);

		for (String label : labels) {
			context.write(new Text(label), new IntWritable(text.size()));
		}
	}

}
