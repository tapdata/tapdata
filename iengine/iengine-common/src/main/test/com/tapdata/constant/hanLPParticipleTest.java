package com.tapdata.constant;

import org.junit.Assert;
import org.junit.Test;

public class hanLPParticipleTest {

	/**
	 * 分词测试
	 */
	@Test
	public void hanLPParticipleTest() {
		String str = "大衛貝克漢不僅僅是名著名球員，球場以外，其妻為前辣妹合唱團成員維多利亞·碧咸，" +
				"亦由於他擁有突出外表、百變髮型及正面的形象，以至自己品牌的男士香水等商品，及長期擔任運動品牌Adidas的代言人，" +
				"因此對大眾傳播媒介和時尚界等方面都具很大的影響力，在足球圈外所獲得的認受程度可謂前所未見。";

		Assert.assertEquals(String.format("HanLP participle %s size not match", ConnectorConstant.CH_SIMPLIFIED_CHINESE),
				53, HanLPUtil.hanLPParticiple(str, ConnectorConstant.CH_SIMPLIFIED_CHINESE).size());
		Assert.assertEquals(String.format("HanLP participle %s size not match", ConnectorConstant.CH_TRADITIONAL_CHINESE),
				45, HanLPUtil.hanLPParticiple(str, ConnectorConstant.CH_TRADITIONAL_CHINESE).size());
		Assert.assertEquals(String.format("HanLP participle %s size not match", ConnectorConstant.HK_TRADITIONAL_CHINESE),
				45, HanLPUtil.hanLPParticiple(str, ConnectorConstant.HK_TRADITIONAL_CHINESE).size());
		Assert.assertEquals(String.format("HanLP participle %s size not match", ConnectorConstant.TW_TRADITIONAL_CHINESE),
				44, HanLPUtil.hanLPParticiple(str, ConnectorConstant.TW_TRADITIONAL_CHINESE).size());
	}
}
