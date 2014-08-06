package zx.soft.naive.bayes.forecast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.naive.bayes.analyzer.AnalyzerTool;

public class SequenceFileToTxt {

	private static Logger logger = LoggerFactory.getLogger(SequenceFileToTxt.class);

	private static String uri = "all-words";
	private static final String fileName = "allwords.txt";
	private static final String negFileName = "negwords.txt";
	//	private static String posFileName = "poswords.txt";
	private static FileWriter writer;

	public static void getWords(String inputFileName, String outputFileName, String[] keywords) throws IOException {

		writer = new FileWriter(outputFileName);
		logger.info("Begin to get words containing " + keywords.toString() + " from " + inputFileName + " to "
				+ outputFileName + "!");
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFileName)));) {
			String aline;
			while ((aline = br.readLine()) != null) {
				for (String keyword : keywords) {
					if (aline.contains(keyword)) {
						writer.write(aline + "\n");
						break;
					}
				}
			}
		} finally {
			writer.close();
			logger.info("Finish getting words containing " + keywords.toString() + " from " + inputFileName + " to "
					+ outputFileName + "!");
		}
	}

	public static void getCityName(String citys, String allwords_without_citys) throws IOException {
		HashSet<String> city_name = new HashSet<>();
		String str = "";
		BufferedReader br = null;
		br = new BufferedReader(new FileReader("city_name"));
		while ((str = br.readLine()) != null) {
			city_name.add(str);
		}
		writer = new FileWriter(citys);
		FileWriter wr = new FileWriter(allwords_without_citys);
		logger.info("Begin to get words containing \n" + city_name + "\n from allwords.txt to " + citys + "!");
		br = new BufferedReader(new InputStreamReader(new FileInputStream("allwords.txt")));
		String aline;
		while ((aline = br.readLine()) != null) {
			for (String city : city_name) {
				if (aline.contains(city)) {
					writer.write(aline + "\n");
					break;
				} else {
					wr.write(aline + "\n");
				}
			}
		}
		writer.close();
		logger.info("Finish getting words containing \n" + city_name + "\n from allwords.txt to " + citys + "!");
	}

	public static void convertSequenceFileToTxt(String outputFileName) throws IOException {
		writer = new FileWriter(outputFileName);
		uri += "/part-r-00000";
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(FileSystem.get(URI.create(uri), conf), new Path(uri), conf);
			Text word = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable count = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while (reader.next(word, count)) {
				writer.write(word.toString() + "      " + count.toString() + "\n");
			}
		} finally {
			IOUtils.closeStream(reader);
			writer.close();
		}
	}

	public static void getNegwordsInScoreTable() throws FileNotFoundException, IOException {
		AnalyzerTool analyzerTool = new AnalyzerTool();
		HashSet<String> uniqueWords = new HashSet<>();
		String str;
		try (BufferedReader br = new BufferedReader(new FileReader(new File("words")));) {
			while ((str = br.readLine()) != null) {
				//				System.out.println(str);
				for (String word : analyzerTool.analyzerTextToList(str)) {
					uniqueWords.add(word);
				}
			}
		}
		analyzerTool.close();
		HashSet<String> allWords = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(new File("allwords.txt")));) {
			while ((str = br.readLine()) != null) {
				allWords.add(str.split("\\s")[0]);
			}
		}
		String words = "";
		for (String word : uniqueWords) {
			if (!allWords.contains(word)) {
				words += '"' + word + "\", ";
			}
		}
		words = words.substring(0, words.length() - 2);
		//		"公共安全", "警徽", "集会", "执法者", "抗法", "黑钱", "游行", "队", "示威游行", "游行示威",
		//		"收", "镇压", "尸", "强", "法警", "安全局", "退休职工", "治安员", "护卫", "协", "网", "羁押",
		//		"女警", "民办教师", "自焚", "慢", "督", "警", "杨", "j", "警校", "酷刑", "刑讯逼供", "佳",
		//		"长丰县", "死", "死于", "辅", "老警", "庐江县", "抢", "交", "出警", "巡警", "知法犯法", "扣押",
		//		"炸", "警衔", "海警", "拆", "补偿款", "警车", "监", "大盖帽", "清场", "以权谋私", "持证", "蜀山区",
		//		"退休工人", "贿赂", "复员费", "警察局长", "专案组"
		System.out.println(words);
	}

	public static void main(String[] args) throws IOException {
		//		获取与地名相关的词语
		//		getCityName("citys", "allwords_without_citys");
		//		评分表中的负面词整理
		String[] negwordsInScoreTable = { "妨碍公共安全", "抗法", "黑钱", "游行", "示威游行", "游行示威", "镇压", "尸", "法警", "安全局", "退休职工",
				"治安员", "网", "羁押", "女警", "民办教师", "自焚", "警", "警校", "酷刑", "刑讯逼供", "死", "老警", "出警", "巡警", "知法犯法", "扣押",
				"炸", "警衔", "海警", "补偿款", "清场", "以权谋私", "退休工人", "贿赂", "警察局长", "专案组" };
		//		getNegwordsInScoreTable();
		String[] negwords = { "冲突", "祸", "破坏", "杀戮", "屠杀", "恐慌", "洪水", "灾", "匪", "亡", "瘾", "下等", "下流", "地狱", "炸", "毒",
				"跳楼", "新疆", "死", "痛苦", "恨", "忿", "愤", "哀", "悲", "怒", "愁", "烂", "杀", "刃", "匕首" };
		//		getWords(fileName, negFileName, negwords);
	}
}
