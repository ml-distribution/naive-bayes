package zx.soft.navie.bayes.mapred;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ClassifyMapperTest {

	private MapDriver<Text, Text, LongWritable, Text> mapDriver;

	@Before
	public void setUp() {
		ClassifyMapper mapper = new ClassifyMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	@Ignore("未完待续")
	public void testClassifyMapper() {
		mapDriver.withInput(new Text(""), new Text(""));
		mapDriver.withOutput(new LongWritable(), new Text(""));
		mapDriver.runTest();
	}

}
