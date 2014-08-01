package zx.soft.naive.bayes.mapred.db;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MergeWordsReducerTest {
	private ReduceDriver<Text, LongWritable,  Text, LongWritable> reduceDriver;
	@Before
	public void setUp() throws Exception {
		MergeWordsReducer reducer = new MergeWordsReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testReduceTextIterableOfLongWritableContext() {
		List<LongWritable> values = new ArrayList<>();
		values.add(new LongWritable(1));
		values.add(new LongWritable(2));
		reduceDriver.withInput(new Text("测试"), values);
		reduceDriver.withOutput(new Text("测试"), new LongWritable(3));
		reduceDriver.runTest();
	}
}
