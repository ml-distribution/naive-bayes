package zx.soft.navie.bayes.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.navie.bayes.simple.NavieBayesSimple;

/**
 * 驱动类
 * @author zhumm
 *
 */
public class NavieBayesDriver {

	private static Logger logger = LoggerFactory.getLogger(NavieBayesDriver.class);

	/**
	 * 主函数
	 */
	public static void main(String[] args) {

		if (args.length == 0) {
			System.err.println("Usage: Driver <class-name>");
			System.exit(-1);
		}
		String[] leftArgs = new String[args.length - 1];
		System.arraycopy(args, 1, leftArgs, 0, leftArgs.length);

		switch (args[0]) {
		case "navieBayesSimple":
			logger.info("简单的Navie Bayes实现： ");
			NavieBayesSimple.main(leftArgs);
			break;
		default:
			return;
		}

	}

}
