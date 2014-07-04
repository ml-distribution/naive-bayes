package zx.soft.naive.bayes.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import zx.soft.naive.bayes.mapred.JoinForecastMapper;

public class JoinForecastMapperTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;

	@Before
	public void setUp() {
		JoinForecastMapper mapper = new JoinForecastMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testJoinForecastMapper() {
		mapDriver.withInput(new LongWritable(2L), new Text("cate1,cate2 测试 数据 效果"));
		mapDriver.withOutput(new Text("测试"), new Text("2,cate1,cate2"));
		mapDriver.withOutput(new Text("数据"), new Text("2,cate1,cate2"));
		mapDriver.withOutput(new Text("效果"), new Text("2,cate1,cate2"));
		mapDriver.runTest();
	}

}
