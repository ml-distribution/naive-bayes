package zx.soft.naive.bayes.mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import zx.soft.naive.bayes.mapred.JoinModelMapper;

public class JoinModelMapperTest {

	private MapDriver<Text, Text, Text, Text> mapDriver;

	@Before
	public void setUp() {
		JoinModelMapper mapper = new JoinModelMapper();
		mapDriver = MapDriver.newMapDriver(mapper);
	}

	@Test
	public void testJoinModelMapper() {
		mapDriver.withInput(new Text("key"), new Text("value"));
		mapDriver.withOutput(new Text("key"), new Text("value"));
		mapDriver.runTest();
	}

}
