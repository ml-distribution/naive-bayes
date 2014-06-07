package zx.soft.navie.bayes.mapred;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class JoinForecastReducerTest {

	private ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	public void setUp() {
		JoinForecastReducer reducer = new JoinForecastReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testJoinForecastReducer_构建好的训练模型数据() {
		List<Text> values = new ArrayList<>();
		values.add(new Text("cate1:3"));
		values.add(new Text("cate2:5"));
		reduceDriver.withInput(new Text("测试"), values);
		//		reduceDriver.withOutput(null, null);
		reduceDriver.runTest();
	}

	@Test
	public void testJoinForecastReducer_包含文档ID类别列表的预测数据() {
		List<Text> values = new ArrayList<>();
		values.add(new Text("2,cate1,cate2"));
		values.add(new Text("12,cate3,cate4"));
		reduceDriver.withInput(new Text("测试"), values);
		reduceDriver.withOutput(new Text("测试"), new Text("::2,cate1,cate2::12,cate3,cate4"));
		reduceDriver.runTest();
	}

	@Test
	public void testJoinForecastReducer_混合数据() {
		List<Text> values = new ArrayList<>();
		values.add(new Text("2,cate1,cate2"));
		values.add(new Text("12,cate3,cate4"));
		values.add(new Text("cate5:5"));
		reduceDriver.withInput(new Text("测试"), values);
		reduceDriver.withOutput(new Text("测试"), new Text("cate5:5::2,cate1,cate2::12,cate3,cate4"));
		reduceDriver.runTest();
	}

}
