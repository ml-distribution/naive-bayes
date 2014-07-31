package zx.soft.naive.bayes.data;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import zx.soft.naive.bayes.forecast.TrainModel;

public class TrainModelTest {

	private static final TrainModel trainModel = new TrainModel();

	@Test
	public void testCatePirorProbs() {
		assertNotNull(trainModel.getCatePirorProb("amazed"));
		assertNotNull(trainModel.getCatePirorProb("angry"));
		assertNotNull(trainModel.getCatePirorProb("anxious"));
		assertNotNull(trainModel.getCatePirorProb("expect"));
		assertNotNull(trainModel.getCatePirorProb("glad"));
		assertNotNull(trainModel.getCatePirorProb("hate"));
		assertNotNull(trainModel.getCatePirorProb("love"));
		assertNotNull(trainModel.getCatePirorProb("sad"));
	}

	@Test
	public void testWordInCateProbs() {
		assertNotNull(trainModel.getWordInCateProb("黄山", "love"));
		assertNotNull(trainModel.getWordInCateProb("黄山", "expect"));
	}

}
