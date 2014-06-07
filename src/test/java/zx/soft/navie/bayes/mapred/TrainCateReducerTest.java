package zx.soft.navie.bayes.mapred;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TrainCateReducerTest {

	private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		TrainCateReducer reducer = new TrainCateReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testTrainCateReducer() {
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(3));
		values.add(new IntWritable(2));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("cate1"), values);
		reduceDriver.withOutput(new Text("cate1"), new Text("3:6"));
		reduceDriver.runTest();
	}

}
