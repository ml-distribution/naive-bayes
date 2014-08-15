package zx.soft.naive.bayes.mapred.db;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MergeWordsReducerTest {
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() throws Exception {
		MergeWordsReducer reducer = new MergeWordsReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testMergeWordsReducer() {
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		reduceDriver.withInput(new Text("测试"), values);
		reduceDriver.withOutput(new Text("测试"), new IntWritable(3));
		reduceDriver.runTest();
	}
}
