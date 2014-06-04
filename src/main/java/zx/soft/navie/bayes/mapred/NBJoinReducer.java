package zx.soft.navie.bayes.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shannon Quinn
 * 
 * Performs the reduce-side join on the model and test data.
 */
public class NBJoinReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
		String modelLine = null;
		ArrayList<String> documents = new ArrayList<String>();
		for (Text value : values) {
			String line = value.toString();
			if (line.contains(":")) {
				// Line is from the model.
				modelLine = line;
			} else {
				// Contains the document ID with list of labels.
				documents.add(line);
			}
		}

		if (documents.size() > 0) {
			// The only words in the training set we care about are those
			// which appear in the testing set as well. If they don't appear
			// in the testing set...sorry brah.
			if (modelLine == null) {
				modelLine = "";
			}
			StringBuilder output = new StringBuilder();
			output.append(String.format("%s::", modelLine));
			for (String doc : documents) {
				output.append(String.format("%s::", doc));
			}
			String out = output.toString();
			context.write(key, new Text(out.substring(0, out.length() - 2)));
		}
	}

}
