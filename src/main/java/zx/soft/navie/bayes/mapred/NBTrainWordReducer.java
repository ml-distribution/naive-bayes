package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shannon Quinn
 *
 * Associates each word with a list of labels, as well as the number of times
 * that word falls under each label.
 */
public class NBTrainWordReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

		// Update the counter to indicate the size of the vocabulary.
		context.getCounter(NBController.NB_COUNTERS.VOCABULARY_SIZE).increment(1);

		// Loop through the labels.
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		for (Text label : values) {
			String labelKey = label.toString();
			counts.put(labelKey, new Integer(counts.containsKey(labelKey) ? counts.get(labelKey).intValue() + 1 : 1));
		}
		StringBuilder outKey = new StringBuilder();
		for (String label : counts.keySet()) {
			outKey.append(String.format("%s:%s ", label, counts.get(label).intValue()));
		}

		// Write out the Map associated with the word.
		context.write(key, new Text(outKey.toString().trim()));
	}

}
