package zx.soft.navie.bayes.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 获取简单的文本训练数据
 * @author zhumm
 *
 */
public class TextTrainDataFactory implements TrainDataFactory {

	private static Logger logger = LoggerFactory.getLogger(TextTrainDataFactory.class);

	private final File trainFiles; // 训练语料库总路径
	private String[] cates; // 训练语料类别名称集合

	public TextTrainDataFactory(String trainDir) {
		this.trainFiles = new File(trainDir);
		this.cates = this.trainFiles.list();
	}

	/**
	 * 获取类别名称
	 */
	@Override
	public String[] getCates() {
		return this.cates;
	}

	@Override
	public void setCates(String[] cates) {
		this.cates = cates;
	}

	/**
	 * 当前类别下的样本数量
	 * @param cate
	 * @return
	 */
	@Override
	public double numOfSampleInCate(String cate) {
		File classDir = new File(trainFiles.getPath() + File.separator + cate);
		return classDir.list().length;
	}

	/**
	 * 全部的样本数量
	 * @return
	 */
	@Override
	public int totalNumOfSample() {
		int count = 0;
		for (int i = 0; i < cates.length; i++) {
			count += numOfSampleInCate(cates[i]);
		}
		return count;
	}

	/**
	 * 当前类别下的所有训练文本的路径集合
	 * @param cate
	 * @return
	 */
	@Override
	public String[] getPathSet(String cate) {
		File cateDir = new File(trainFiles.getPath() + File.separator + cate);
		String[] ret = cateDir.list();
		for (int i = 0; i < ret.length; i++) {
			ret[i] = trainFiles.getPath() + File.separator + cate + File.separator + ret[i];
		}
		return ret;
	}

	/**
	 * 读取样本内容
	 * @param sampleDir
	 * @return
	 */
	@Override
	public String getSampleContent(String sampleDir) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sampleDir), "GBK"));) {
			String result = "", aline;
			while ((aline = br.readLine()) != null) {
				result = aline + " ";
			}
			return result;
		} catch (IOException e) {
			logger.error("IOException in getText: " + e);
			throw new RuntimeException(e);
		}
	}

}
