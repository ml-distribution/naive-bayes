package zx.soft.navie.bayes.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shannon Quinn
 *
 * Sums the log probabilities for the document.
 */
public class NBClassifyReducer extends Reducer<LongWritable, Text, LongWritable, IntWritable> {

	private long totalDocuments;
	private long uniqueLabels;
	private HashMap<String, Integer> docsWithLabel;

	@Override
	protected void setup(Context context) throws IOException {
		totalDocuments = context.getConfiguration().getLong(NBController.TOTAL_DOCS, 100);
		uniqueLabels = context.getConfiguration().getLong(NBController.UNIQUE_LABELS, 100);
		docsWithLabel = new HashMap<String, Integer>();

		// Build a HashMap of the label data in the DistributedCache.
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length < 1) {
			throw new IOException("DistributedCache returned an empty file set!");
		}

		// Read in from the DistributedCache.
		LocalFileSystem lfs = FileSystem.getLocal(context.getConfiguration());
		for (Path file : files) {
			FSDataInputStream input = lfs.open(file);
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String[] elems = line.split("\\s+");
				String label = elems[0];
				String[] counts = elems[1].split(":");
				docsWithLabel.put(label, new Integer(Integer.parseInt(counts[0])));
			}
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws InterruptedException,
			IOException {
		// Lots of metadata.
		HashMap<String, Double> probabilities = new HashMap<String, Double>();
		ArrayList<String> trueLabels = null;

		for (Text value : values) {
			// Each value is a list of label probabilities for a single word.
			String[] elements = value.toString().split("::");
			String[] labelProbs = elements[0].split(",");
			for (String labelProb : labelProbs) {
				String[] pieces = labelProb.split(":");
				String label = pieces[0];
				double prob = Double.parseDouble(pieces[1]);

				probabilities.put(label, new Double(probabilities.containsKey(label) ? probabilities.get(label)
						.doubleValue() + prob : prob));
			}

			// Also need the true labels.
			if (trueLabels == null) {
				String[] list = elements[1].split(":");
				trueLabels = new ArrayList<String>();
				for (String elem : list) {
					trueLabels.add(elem);
				}
			}
		}

		// Now, loop through each label, adding in the prior for it and 
		// determining what label is most likely.
		double bestProb = Double.NEGATIVE_INFINITY;
		String bestLabel = null;
		for (String label : probabilities.keySet()) {
			double prior = Math.log((double) docsWithLabel.get(label).intValue() + NBController.ALPHA)
					- Math.log(totalDocuments + (NBController.ALPHA * uniqueLabels));
			double totalProb = probabilities.get(label).doubleValue() + prior;
			if (totalProb > bestProb) {
				bestLabel = label;
				bestProb = totalProb;
			}
		}

		// All done! Did we get it right???
		context.write(key, new IntWritable(trueLabels.contains(bestLabel) ? 1 : 0));
	}

}
