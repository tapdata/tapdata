package com.tapdata.constant;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xj
 * 2020-03-31 19:56
 **/
public class HanLPUtil {

	/**
	 * HanLP分词
	 *
	 * @param inputString
	 * @param language
	 * @return
	 */
	public static List<String> hanLPParticiple(String inputString, String language) {

		if (StringUtils.isBlank(inputString)) {
			return new ArrayList<>();
		}

		//标准分词
		//繁体情况，转换为简体
		String participleString;
		switch (language) {
			case ConnectorConstant.CH_TRADITIONAL_CHINESE:
				participleString = HanLP.t2s(inputString);
				break;
			case ConnectorConstant.HK_TRADITIONAL_CHINESE:
				participleString = HanLP.hk2s(inputString);
				break;
			case ConnectorConstant.TW_TRADITIONAL_CHINESE:
				participleString = HanLP.tw2s(inputString);
				break;
			default:
				participleString = inputString;
				break;
		}
		// NotionalTokenizer无介词分词
		List<Term> termList = NotionalTokenizer.segment(participleString);
		List<String> outPutWordList = new ArrayList<>();
		for (Term term : termList) {
			switch (language) {
				case ConnectorConstant.CH_TRADITIONAL_CHINESE: {
					outPutWordList.add(HanLP.s2t(term.word));
				}
				break;
				case ConnectorConstant.HK_TRADITIONAL_CHINESE: {
					outPutWordList.add(HanLP.s2hk(term.word));
				}
				break;
				case ConnectorConstant.TW_TRADITIONAL_CHINESE: {
					outPutWordList.add(HanLP.hk2tw(HanLP.s2hk(term.word)));
				}
				break;
				default: {
					outPutWordList.add(term.word);
				}
				break;
			}
		}
		return outPutWordList;
	}
}
