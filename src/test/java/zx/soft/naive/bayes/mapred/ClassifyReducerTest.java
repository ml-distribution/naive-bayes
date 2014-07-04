package zx.soft.naive.bayes.mapred;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import zx.soft.naive.bayes.mapred.ClassifyTestReducer;

public class ClassifyReducerTest {

	private ReduceDriver<LongWritable, Text, LongWritable, IntWritable> reduceDriver;

	@Before
	public void setUp() {
		ClassifyTestReducer reducer = new ClassifyTestReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	@Ignore("未完待续")
	public void testClassifyReducer() {
		List<Text> values = new ArrayList<>();
		values.add(new Text(""));
		values.add(new Text(""));
		values.add(new Text(""));
		reduceDriver.withInput(new LongWritable(), values);
		reduceDriver.withOutput(new LongWritable(), new IntWritable());
		reduceDriver.runTest();
	}
}
