package zx.soft.navie.bayes.data;

/**
 * 训练集工厂接口
 * @author zhumm
 *
 */
public interface TrainDataFactory {

	/**
	 * 获取类别名称
	 */
	public String[] getCates();

	public void setCates(String[] cates);

	/**
	 * 当前类别下的样本数量
	 * @param cate
	 * @return
	 */
	public double numOfSampleInCate(String cate);

	/**
	 * 全部的样本数量
	 * @return
	 */
	public int totalNumOfSample();

	/**
	 * 当前类别下的所有训练文本的路径集合
	 * @param cate
	 * @return
	 */
	public String[] getPathSet(String cate);

	/**
	 * 读取样本内容
	 * @param fileDir
	 * @return
	 */
	public String getSampleContent(String sampleDir);

}
