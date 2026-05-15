package com.tapdata.constant;

import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.utils.AppType;

import java.util.List;
import java.util.function.Supplier;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 17:32
 **/
public class ClientOperatorUtil {
	public static HttpClientMongoOperator buildHttpClientMongoOperator(ConfigurationCenter configurationCenter) {

		List<String> baseURLs = (List<String>) configurationCenter.getConfig(ConfigurationCenter.BASR_URLS);

		int retryTime = (int) configurationCenter.getConfig(ConfigurationCenter.RETRY_TIME);

		int connectTimeout = (int) CommonUtils.getPropertyLong("REST_CONNECT_TIMEOUT", 5000L);
		int readTimeout = (int) CommonUtils.getPropertyLong("REST_READ_TIMEOUT", 15000L);
		int connectRequestTimeout = (int) CommonUtils.getPropertyLong("REST_CONNECT_REQUEST_TIMEOUT", 5000L);
		Supplier<Long> getRetryTimeout = () -> {
			long defRestApiTimeout = CommonUtils.getPropertyLong("DEFAULT_REST_API_TIMEOUT", AppType.currentType().isCloud() ? 300000L : 60000L);
			long minRestApiTimeout = CommonUtils.getPropertyLong("MINIMUM_REST_API_TIMEOUT", 30000L);
			long maxRestApiTimeout = CommonUtils.getPropertyLong("MAX_REST_API_TIMEOUT", AppType.currentType().isCloud() ? 300000L : 120000L);
			return Math.min(Math.max((long) (defRestApiTimeout * 0.8), minRestApiTimeout), maxRestApiTimeout);
		};

		RestTemplateOperator restTemplateOperator = new RestTemplateOperator(
				baseURLs, retryTime, getRetryTimeout,
				connectTimeout, readTimeout, connectRequestTimeout);
		return new HttpClientMongoOperator(
				null,
				null,
				restTemplateOperator,
				configurationCenter
		);
	}
}
