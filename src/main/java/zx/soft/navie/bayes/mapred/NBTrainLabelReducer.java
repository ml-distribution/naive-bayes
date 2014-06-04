package zx.soft.navie.bayes.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shannon Quinn
 * 
 * Tabulates some counts under each label. The output will be serialized
 * in the DistributedCache for access by the classifier after this step.
 */
public class NBTrainLabelReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException,
			IOException {

		// Each time this Reducer is invoked, that means we have another unique label.
		context.getCounter(NBController.NB_COUNTERS.UNIQUE_LABELS).increment(1);

		long pY = 0;
		long pYW = 0;
		for (IntWritable value : values) {
			// Increment the global counter (Y = *).
			context.getCounter(NBController.NB_COUNTERS.TOTAL_DOCS).increment(1);

			// Increment the number of documents with this label (Y = y).
			pY++;

			// Increment the number of words under this label.
			pYW += value.get();
		}

		// Write out the results in the format:
		// <label Y> {<# of documents with label Y>:<# of words under label Y>} 
		context.write(key, new Text(String.format("%s:%s", pY, pYW)));
	}

}
