package zx.soft.naive.bayes.mapred;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import zx.soft.naive.bayes.mapred.TrainWordReducer;

public class TrainWordReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		TrainWordReducer reducer = new TrainWordReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testTrainWordReducer() {
		List<Text> values = new ArrayList<>();
		values.add(new Text("cate1"));
		values.add(new Text("cate2"));
		values.add(new Text("cate3"));
		values.add(new Text("cate1"));
		values.add(new Text("cate2"));
		values.add(new Text("cate1"));
		reduceDriver.withInput(new Text("测试"), values);
		reduceDriver.withOutput(new Text("测试"), new Text("cate1:3 cate2:2 cate3:1"));
		reduceDriver.runTest();
	}

}
