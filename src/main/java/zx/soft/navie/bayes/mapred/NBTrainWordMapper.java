package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 *
 * Reads and parses the input files to build a reverse index of words to labels.
 */
public class NBTrainWordMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String[] words = value.toString().split("\\s+");
		Vector<String> labels = NBController.tokenizeLabels(words[0]);
		Vector<String> text = NBController.tokenizeDoc(words);

		for (String label : labels) {
			for (String word : text) {
				// (Y = y, W = w)
				context.write(new Text(word), new Text(label));
			}
		}
	}

}
