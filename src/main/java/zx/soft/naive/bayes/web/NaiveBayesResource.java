package zx.soft.naive.bayes.web;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.naive.bayes.utils.URLCodecUtils;

/**
 * 情感分类资源类
 * 
 * @author wanggang
 *
 */
public class NaiveBayesResource extends ServerResource {

	private static Logger logger = LoggerFactory.getLogger(NaiveBayesResource.class);

	private NaiveBayesApplication application;

	private String text = "";

	@Override
	public void doInit() {
		text = (String) getRequest().getAttributes().get("text");
		application = (NaiveBayesApplication) getApplication();
	}

	@Get("txt")
	public Object returnCate() {
		logger.info("Request Url: " + URLCodecUtils.decoder(getReference().toString(), "utf-8") + ".");
		if (text == null || text.length() == 0) {
			return "others";
		}
		return application.classify(text);
	}

}
