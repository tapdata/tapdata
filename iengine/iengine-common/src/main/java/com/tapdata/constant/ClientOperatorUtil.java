package com.tapdata.constant;

import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 17:32
 **/
public class ClientOperatorUtil {
	public static HttpClientMongoOperator buildHttpClientMongoOperator(ConfigurationCenter configurationCenter) {

		List<String> baseURLs = (List<String>) configurationCenter.getConfig(ConfigurationCenter.BASR_URLS);

		int retryTime = (int) configurationCenter.getConfig(ConfigurationCenter.RETRY_TIME);

		RestTemplateOperator restTemplateOperator = new RestTemplateOperator(baseURLs, retryTime);
		return new HttpClientMongoOperator(
				null,
				null,
				restTemplateOperator,
				configurationCenter
		);
	}
}
