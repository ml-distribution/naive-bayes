package zx.soft.naive.bayes.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import zx.soft.naive.bayes.mapred.TrainCateMapper;

public class TrainCateMapperTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

	@Before
	public void setUp() {
		TrainCateMapper mapper = new TrainCateMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testTrainCateMapper() {
		mapDriver.withInput(new LongWritable(2L), new Text("cate1,cate2 测试 数据 分布式"));
		mapDriver.withOutput(new Text("cate1"), new IntWritable(3));
		mapDriver.withOutput(new Text("cate2"), new IntWritable(3));
		mapDriver.runTest();
	}

}
