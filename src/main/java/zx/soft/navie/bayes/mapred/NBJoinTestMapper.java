package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 *
 * Prepares the test data to be joined with the model for classification.
 */
public class NBJoinTestMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String[] words = value.toString().split("\\s+");
		Vector<String> labels = NBController.tokenizeLabels(words[0]);
		Vector<String> text = NBController.tokenizeDoc(words);

		StringBuilder labelString = new StringBuilder();
		for (String label : labels) {
			labelString.append(String.format("%s,", label));
		}
		String output = labelString.toString();

		// Output each word and its list of labels.
		for (String word : text) {
			context.write(new Text(word),
					new Text(String.format("%s,%s", key.get(), output.substring(0, output.length() - 1))));
		}
	}

}
