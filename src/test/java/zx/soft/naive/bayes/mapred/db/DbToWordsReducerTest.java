package zx.soft.naive.bayes.mapred.db;

import static org.junit.Assert.assertEquals;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

public class DbToWordsReducerTest {
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
	public void DbToWordsReducer() {
		String weibo = "【新浪微博iPhone客户端4.0版全新发布】";
		String[] words = analyzerTool.analyzerTextToArr(weibo);
		String regex = "^[0-9a-zA-Z_]";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = null;
		String result = "";
		for (String word : words) {
			matcher = pattern.matcher(word);
			if (!matcher.find()) {
				result += word + " ";
			}
		}
		assertEquals("新浪 微 博 客户端 全新 发布 ", result);
	}
}
