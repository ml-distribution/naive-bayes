package zx.soft.navie.bayes.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Shannon Quinn
 * 
 * Generates a probability for each word for each of the labels under
 * which it has been observed.
 */
public class NBClassifyMapper extends Mapper<Text, Text, LongWritable, Text> {

	private long vocabularySize;
	private HashMap<String, Integer> wordsUnderLabel;

	@Override
	protected void setup(Context context) throws IOException {
		vocabularySize = context.getConfiguration().getLong(NBController.VOCABULARY_SIZE, 100);
		wordsUnderLabel = new HashMap<String, Integer>();

		// Build a HashMap of the label data in the DistributedCache.
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length < 1) {
			throw new IOException("DistributedCache returned an empty file set!");
		}

		// Read in the shards from the DistributedCache.
		LocalFileSystem lfs = FileSystem.getLocal(context.getConfiguration());
		for (Path file : files) {
			FSDataInputStream input = lfs.open(file);
			BufferedReader in = new BufferedReader(new InputStreamReader(input));
			String line;
			while ((line = in.readLine()) != null) {
				String[] elems = line.split("\\s+");
				String label = elems[0];
				String[] counts = elems[1].split(":");
				wordsUnderLabel.put(label, new Integer(Integer.parseInt(counts[1])));
			}
			IOUtils.closeStream(in);
		}
	}

	@Override
	public void map(Text key, Text value, Context context) throws InterruptedException, IOException {
		String[] elements = value.toString().split("::");
		String model = elements[0];

		// Create a HashMap for the model counts.
		HashMap<String, Integer> modelCounts = null;
		if (model.length() > 0) {
			String[] labelCounts = model.split(" ");
			modelCounts = new HashMap<String, Integer>();
			for (String labelCount : labelCounts) {
				String[] elems = labelCount.split(":");
				modelCounts.put(elems[0], new Integer(Integer.parseInt(elems[1])));
			}
		}

		// How many times did this word show up in each document?
		HashMap<Long, Integer> multipliers = new HashMap<Long, Integer>();
		HashMap<Long, String> trueLabels = new HashMap<Long, String>();
		for (int i = 1; i < elements.length; ++i) {
			String[] elems = elements[i].split(",");
			Long docId = new Long(Long.parseLong(elems[0]));
			multipliers.put(docId, new Integer(multipliers.containsKey(docId) ? multipliers.get(docId).intValue() + 1
					: 1));
			if (!trueLabels.containsKey(docId)) {
				// Add the list of true labels for this document.
				// ASSUMPTION: The same document ID will have the same true labels.
				StringBuilder list = new StringBuilder();
				for (int j = 1; j < elems.length; ++j) {
					list.append(String.format("%s:", elems[j]));
				}
				String outval = list.toString();
				trueLabels.put(docId, outval.substring(0, outval.length() - 1));
			}
		}

		// Now loop through each label, calculating the probability of the
		// word under that label for each document ID.
		for (Long docId : trueLabels.keySet()) {
			StringBuilder probs = new StringBuilder();
			for (String label : wordsUnderLabel.keySet()) {
				int wordLabelCount = wordsUnderLabel.get(label).intValue();
				int count = 0;
				if (modelCounts != null && modelCounts.containsKey(label)) {
					count = modelCounts.get(label).intValue();
				}

				int multiplier = multipliers.get(docId);
				double wordProb = (double) multiplier
						* (Math.log(count + NBController.ALPHA) - Math.log(wordLabelCount
								+ (NBController.ALPHA * vocabularySize)));
				probs.append(String.format("%s:%s,", label, wordProb));
			}
			// Output the document ID, followed by the value containing:
			// list of "<label:probability>," statements for the word
			// list of "<truelabel>:" statements for each true label of the document
			String output = probs.toString();
			context.write(new LongWritable(docId),
					new Text(String.format("%s::%s", output.substring(0, output.length() - 1), trueLabels.get(docId))));
		}
	}

}
