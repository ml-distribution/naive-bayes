package zx.soft.naive.bayes.web;

import java.util.Properties;

import org.restlet.Component;
import org.restlet.data.Protocol;

import zx.soft.naive.bayes.jackson.ReplaceConvert;
import zx.soft.naive.bayes.utils.ConfigUtil;

/**
 * 
 * http://localhost:XXXX/sentiment/classify/{text}
 * 
 * @author wanggang
 *
 */
public class NaiveBayesServer {

	private final Component component;
	private final NaiveBayesApplication naiveBayesApplication;

	private final int PORT;

	public NaiveBayesServer() {
		Properties props = ConfigUtil.getProps("web-server.properties");
		PORT = Integer.parseInt(props.getProperty("api.port"));
		component = new Component();
		naiveBayesApplication = new NaiveBayesApplication();
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {

		NaiveBayesServer server = new NaiveBayesServer();
		server.start();

	}

	public void start() {
		component.getServers().add(Protocol.HTTP, PORT);
		try {
			component.getDefaultHost().attach("/sentiment", naiveBayesApplication);
			ReplaceConvert.configureJacksonConverter();
			component.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void stop() {
		try {
			component.stop();
			naiveBayesApplication.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
