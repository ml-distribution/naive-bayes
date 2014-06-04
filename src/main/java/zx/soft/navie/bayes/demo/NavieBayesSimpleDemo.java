package zx.soft.navie.bayes.demo;

import zx.soft.navie.bayes.data.TextTrainDataFactory;
import zx.soft.navie.bayes.simple.NavieBayesSimple;

public class NavieBayesSimpleDemo {

	public static void main(String[] args) {

		//String text = "微软公司提出以446亿美元的价格收购雅虎中国网2月1日报道 美联社消息，微软公司提出以446亿美元现金加股票的价格收购搜索网站雅虎公司。微软提出以每股31美元的价格收购雅虎。微软的收购报价较雅虎1月31日的收盘价19.18美元溢价62%。微软公司称雅虎公司的股东可以选择以现金或股票进行交易。微软和雅虎公司在2006年底和2007年初已在寻求双方合作。而近两年，雅虎一直处于困境：市场份额下滑、运营业绩不佳、股价大幅下跌。对于力图在互联网市场有所作为的微软来说，收购雅虎无疑是一条捷径，因为双方具有非常强的互补性。(小桥)";
		String text = "联想THINKPAD近期几乎全系列笔记本电脑降价促销，最高降幅达到800美元，降幅达到42%。这是记者昨天从联想美国官方网站发现的。联想相关人士表示，这是为纪念新联想成立1周年而在美国市场推出的促销，产品包括THINKPADT、X以及Z系列笔记本。促销不是打价格战，THINK品牌走高端商务路线方向不会改变";
		String transDir = "sample";
		NavieBayesSimple bayes = new NavieBayesSimple(new TextTrainDataFactory(transDir));
		System.out.println(bayes.classifyText(text));

	}

}
