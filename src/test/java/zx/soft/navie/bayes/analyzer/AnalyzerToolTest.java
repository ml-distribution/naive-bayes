package zx.soft.navie.bayes.analyzer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

//@FixMethodOrder(MethodSorters.DEFAULT)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AnalyzerToolTest {

	private static AnalyzerTool analyzerTool;

	@BeforeClass
	public static void init() {
		System.out.println("start...");
		analyzerTool = new AnalyzerTool();
	}

	@AfterClass
	public static void close() {
		System.out.println("after...");
		analyzerTool.close();
	}

	@Test
	public void testAnalyzerTextToList_分词结果转换成列表() {
		assertTrue(analyzerTool.analyzerTextToList("我去年买了个表").size() > 0);
		assertNotNull("返回不为空", analyzerTool.analyzerTextToList("我去年买了个表"));
	}

	@Test
	public void testAnalyzerTextToStr_分词结果转换成字符串() {
		assertTrue(analyzerTool.analyzerTextToStr("我去年买了个表", ",").contains(","));
		assertTrue(analyzerTool.analyzerTextToStr("我去年买了个表", ",").length() > 0);
	}

	@Test
	public void testAnalyzerTextArr_分词结果转换成数组() {
		assertTrue(analyzerTool.analyzerTextToArr("我去年买了个表").length > 0);
	}

}
