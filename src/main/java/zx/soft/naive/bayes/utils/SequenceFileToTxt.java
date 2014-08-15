package zx.soft.naive.bayes.utils;

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

/**
 * 对多种途径人工挑选的词典进行合并
 * 并结合之前mapreduce分词后的结果挑选出相关的负面词库，正面词库等
 * 最主要的输出是：
 * dicts/negwords
 * dicts/poswords
 * dicts/neuralwords
 * @author frank
 *
 */
public class SequenceFileToTxt {

	private static Logger logger = LoggerFactory.getLogger(SequenceFileToTxt.class);

	private static String prefixPath = "dicts/";
	private static String uri = "all-words";
	private static final String negFileName = "negwords";
	private static final String posFileName = "poswords";
	private static FileWriter writer;

	/**
	 * 从SequenceFile中读出所有单词到allwords中
	 * @param inputFileName
	 * @param outputFileName
	 * @param keywords
	 * @throws IOException
	 */
	public static void convertSequenceFileToTxt(String outputFileName) throws IOException {
		writer = new FileWriter(prefixPath + outputFileName);
		uri += "/part-r-00000";
		Configuration conf = new Configuration();
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(FileSystem.get(URI.create(uri), conf), new Path(uri), conf);
			Text word = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			IntWritable count = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			logger.info("Begin to convert all-words/part-r-00000 to " + prefixPath + "allwords!");
			while (reader.next(word, count)) {
				writer.write(word.toString() + "      " + count.toString() + "\n");
			}
		} finally {
			IOUtils.closeStream(reader);
			writer.close();
			logger.info("Finishing converting all-words/part-r-00000 to " + prefixPath + "allwords!");
		}
	}

	/**
	 * 将所有词语分为与地名相关的allwords_with_citys和与地名不相关的allwords_without_citys
	 * 后面在allwords_without_citys基础上进行分词
	 * @param allwords_with_citys
	 * @param allwords_without_citys
	 * @throws IOException
	 */
	public static void getCityName(String allwords_with_citys, String allwords_without_citys) throws IOException {
		HashSet<String> city_name = new HashSet<>();
		String str = "", aline = "";
		BufferedReader br = null;
		boolean isCity = false;

		writer = new FileWriter(prefixPath + allwords_with_citys);
		FileWriter wr = new FileWriter(prefixPath + allwords_without_citys);

		br = new BufferedReader(new FileReader(prefixPath + "city_name"));
		while ((str = br.readLine()) != null) {
			if (!str.trim().equals("")) {
				city_name.add(str.trim());
			}
		}
		br.close();

		logger.info("Begin to split " + prefixPath + "allwords into " + prefixPath + allwords_with_citys + " and "
				+ prefixPath + allwords_without_citys + "!");
		br = new BufferedReader(new InputStreamReader(new FileInputStream(prefixPath + "allwords")));
		while ((aline = br.readLine()) != null) {
			isCity = false;
			for (String city : city_name) {
				if (aline.contains(city)) {
					isCity = true;
					writer.write(aline + "\n");
					break;
				}
			}
			if (!isCity)
				wr.write(aline + "\n");
		}
		br.close();

		writer.close();
		wr.close();
		logger.info("Finish splitting " + prefixPath + "allwords into " + prefixPath + allwords_with_citys + " and "
				+ prefixPath + allwords_without_citys + "!");
	}

	/**
	 * 提取出现在评分表中却没出现在allwords中的负面词
	 * @param negwordsInScoreTable
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void getNegwordsInScoreTable(String negwordsInScoreTable) throws FileNotFoundException, IOException {
		AnalyzerTool analyzerTool = new AnalyzerTool();
		HashSet<String> uniqueWords = new HashSet<>();
		String str = "";
		try (BufferedReader br = new BufferedReader(new FileReader(new File(prefixPath + "wordsInScoreTable")));) {
			while ((str = br.readLine()) != null) {
				for (String word : analyzerTool.analyzerTextToList(str)) {
					uniqueWords.add(word);
				}
			}
		}
		analyzerTool.close();
		HashSet<String> allWords = new HashSet<>();
		try (BufferedReader br = new BufferedReader(new FileReader(new File(prefixPath + "allwords_without_citys")));) {
			while ((str = br.readLine()) != null) {
				if (!str.trim().equals("")) {
					allWords.add(str.trim().split("\\s+")[0]);
				}
			}
		}

		writer = new FileWriter(prefixPath + negwordsInScoreTable);
		String words = "";
		logger.info("Begin to retrieve negwords in " + prefixPath + "wordsInScoreTable into " + prefixPath
				+ negwordsInScoreTable + "!");
		for (String word : uniqueWords) {
			if (!allWords.contains(word)) {
				words += '"' + word + "\", ";
				writer.write(word + "\n");
			}
		}
		writer.close();
		logger.info("Finish retrieving negwords in " + prefixPath + "wordsInScoreTable into " + prefixPath
				+ negwordsInScoreTable + "!");
		words = words.substring(0, words.length() - 2);
		//		System.out.println(words);
	}

	/**
	 * 从allwords_without_citys中提取所有相关的词语
	 * @param args
	 * @throws IOException
	 */
	public static void getWords(String inputFileName, String outputFileName, HashSet<String> keywords)
			throws IOException {

		writer = new FileWriter(prefixPath + outputFileName);
		logger.info("Begin to retrieve negwords in " + prefixPath + inputFileName + " into " + prefixPath
				+ outputFileName + "!");
		HashSet<String> words = new HashSet<String>();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(prefixPath
				+ inputFileName)));) {
			String aline = "";
			String word = "";
			while ((aline = br.readLine()) != null) {
				if (!aline.trim().equals("")) {
					word = aline.trim().split(" +")[0];
					for (String keyword : keywords) {
						if (word.contains(keyword)) {
							words.add(word);
							break;
						}
					}
				}
			}
			for (String keyword : keywords) {
				words.add(keyword);
			}
			for (String w : words) {
				writer.write(w + "\n");
			}
		} finally {
			writer.close();
			logger.info("Finishing retrieving negwords in " + prefixPath + inputFileName + " into " + prefixPath
					+ outputFileName + "!");
		}
	}

	/**
	 * 将人工选择出来的词存入hashset去重，便于进行遍历
	 * @param poswords
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static HashSet<String> readWordsIntoSet(HashSet<String> poswords, String fileName) throws IOException {
		String str = "";
		BufferedReader br = new BufferedReader(new FileReader(prefixPath + fileName));
		while ((str = br.readLine()) != null) {
			if (!str.trim().equals("")) {
				poswords.add(str.trim());
			}
		}
		br.close();
		return poswords;
	}

	/**
	 * 从allwords中去掉poswords和negwords得到中性词
	 * @param inputFileName
	 * @param outputFileName
	 * @param negFileName
	 * @param posFileName
	 * @throws IOException
	 */
	public static void getNeutralWords(String inputFileName, String outputFileName, String negFileName,
			String posFileName) throws IOException {
		String str = "";
		HashSet<String> negwords = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(prefixPath + negFileName));
		while ((str = br.readLine()) != null) {
			negwords.add(str.trim());
		}
		br.close();

		HashSet<String> poswords = new HashSet<String>();
		br = new BufferedReader(new FileReader(prefixPath + posFileName));
		while ((str = br.readLine()) != null) {
			poswords.add(str.trim());
		}
		br.close();

		writer = new FileWriter(prefixPath + outputFileName);

		br = new BufferedReader(new FileReader(prefixPath + inputFileName));
		String word = "";
		logger.info("Begin to retrieve neuralwords in " + prefixPath + inputFileName + " into " + prefixPath
				+ outputFileName + "!");
		while ((str = br.readLine()) != null) {
			if (!str.trim().equals("")) {
				word = str.trim().split(" +")[0];
				if (!negwords.contains(word) && !poswords.contains(word)) {
					writer.write(word + "\n");
				}
			}
		}
		br.close();
		logger.info("Finishing retrieving neuralwords in " + prefixPath + inputFileName + " into " + prefixPath
				+ outputFileName + "!");
	}

	/**
	 * 将不同输入分词文件合并到新的输入文件，去除空行和重复
	 * @param inputFileNames
	 * @param outputFileName
	 * @throws IOException
	 */
	public static void mergeWordFilesAndRemoveDuplicatesAndBlanklines(String[] inputFileNames, String outputFileName)
			throws IOException {
		String str = "";
		HashSet<String> words = new HashSet<String>();
		BufferedReader br = null;
		for (String inputFileName : inputFileNames) {
			br = new BufferedReader(new FileReader(prefixPath + inputFileName));
			while ((str = br.readLine()) != null) {
				if (!str.trim().equals("")) {
					words.add(str.trim());
				}
			}
			br.close();
		}
		writer = new FileWriter(prefixPath + outputFileName);
		for (String word : words) {
			writer.write(word + "\n");
		}
		writer.close();
		System.out.println("Finish mergeWordFiles!");
	}

	public static void main(String[] args) throws IOException {
		//		将所有词语分为与地名相关的allwords_with_citys和与地名不相关的allwords_without_citys
		//		getCityName("allwords_with_citys", "allwords_without_citys");
		//		提取出现在评分表中却没出现在allwords中的负面词
		//		String[] negwordsInScoreTable = { "妨碍公共安全", "抗法", "黑钱", "游行", "示威游行", "游行示威", "镇压", "尸", "法警", "安全局", "退休职工",
		//				"治安员", "法网", "羁押", "女警", "民办教师", "自焚", "警", "警校", "酷刑", "刑讯逼供", "死", "老警", "出警", "巡警", "知法犯法", "扣押",
		//				"炸", "警衔", "海警", "补偿款", "清场", "以权谋私", "退休工人", "贿赂", "警察局长", "专案组" };
		//		getNegwordsInScoreTable("negwordsInScoreTable");

		String[] inputFileNames1 = { "negwords_manpicked_fuhh", "negwords_manpicked_zhumm" };
		mergeWordFilesAndRemoveDuplicatesAndBlanklines(inputFileNames1, "negwords_manpicked");

		String[] inputFileNames2 = { "poswords_manpicked_fuhh", "poswords_manpicked_zhumm" };
		mergeWordFilesAndRemoveDuplicatesAndBlanklines(inputFileNames2, "poswords_manpicked");

		HashSet<String> negwords = new HashSet<String>();
		//
		HashSet<String> poswords = new HashSet<String>();
		//
		poswords = readWordsIntoSet(poswords, "poswords_manpicked");
		negwords = readWordsIntoSet(negwords, "negwords_manpicked");
		//
		getWords("allwords_without_citys", negFileName, negwords);
		getWords("allwords_without_citys", posFileName, poswords);

		//普通poswords加上否定词可能会影响负面分值。
		getNeutralWords("allwords_without_citys", "neuralwords", negFileName, posFileName);

	}
}
