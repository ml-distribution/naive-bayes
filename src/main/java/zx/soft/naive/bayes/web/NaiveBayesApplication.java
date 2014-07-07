package zx.soft.naive.bayes.web;

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;

import zx.soft.naive.bayes.forecast.ForecastCore;

/**
 * 情感分类应用类
 * 
 * @author wanggang
 *
 */
public class NaiveBayesApplication extends Application {

	public final ForecastCore forecastCore;

	public NaiveBayesApplication() {
		forecastCore = new ForecastCore();
	}

	@Override
	public Restlet createInboundRoot() {
		Router router = new Router(getContext());
		router.attach("/classify/{text}", NaiveBayesResource.class);
		return router;
	}

	/**
	 * 分类
	 */
	public String classify(String text) {
		return forecastCore.classify(text);
	}

	public void close() {
		forecastCore.close();
	}

}
