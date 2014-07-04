package zx.soft.naive.bayes.mapred;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import zx.soft.naive.bayes.mapred.TrainsVector;

public class TrainsVectorTest {

	@Test
	public void testTokenizeDoc() {
		String[] words = { "cate1,cate2", "测试", "数据" };
		assertEquals("[测试, 数据]", TrainsVector.tokenizeDoc(words).toString());
	}

	@Test
	public void testTokenizeCates() {
		String cates = "cate1,cate2";
		assertEquals("[cate1, cate2]", TrainsVector.tokenizeCates(cates).toString());
	}

}
