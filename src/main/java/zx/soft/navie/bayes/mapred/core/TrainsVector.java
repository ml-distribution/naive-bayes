package zx.soft.navie.bayes.mapred.core;

import java.util.Vector;

public class TrainsVector {

	/**
	 * 将一个文档下的词汇数组转换成向量
	 * @param words
	 * @return
	 */
	public static Vector<String> tokenizeDoc(String[] words) {
		Vector<String> tokens = new Vector<String>();
		for (int i = 1; i < words.length; i++) {
			if (words[i].length() > 0) {
				tokens.add(words[i]);
			}
		}
		return tokens;
	}

	/**
	 * 将一个文档下的类别数组转换成向量
	 * @param cates
	 * @return
	 */
	public static Vector<String> tokenizeCates(String cates) {
		String[] tokens = cates.split(",");
		Vector<String> retval = new Vector<String>();
		for (String token : tokens) {
			retval.add(token);
		}
		return retval;
	}

}
