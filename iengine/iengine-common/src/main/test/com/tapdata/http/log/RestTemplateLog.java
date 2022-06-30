package com.tapdata.http.log;

import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.LoginResp;
import com.tapdata.entity.ResponseBody;
import com.tapdata.interceptor.LoggingInterceptor;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/3/23 下午8:45
 * @description
 */
public class RestTemplateLog {

	private RestTemplate restTemplate;

	public static void main(String[] args) {
		//System.setProperty("log4j2.debug", "true");
		//new RestTemplateLog().test();
		setLog();
		RestTemplateLog testCase = new RestTemplateLog();
		testCase.test();
		testCase.testFindOne();
		;
	}

	private static void setLog() {
		Level defaultLogLevel = Level.DEBUG;

		final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
		/*PatternLayout patternLayout = PatternLayout.newBuilder()
			.withPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} - %msg%n")
			.build();*/

		/*ConsoleAppender consoleAppender = ConsoleAppender.newBuilder()
			.withName("consoleAppender")
			.withLayout(patternLayout)
			.withImmediateFlush(true)
			.build();*/
//
//        rootLogger.addAppender(rollingFileAppender);

		//AppenderRef ref = AppenderRef.createAppenderRef("rollingFileAppender", defaultLogLevel, null);
		LoggerConfig rootLoggerConfig = config.getRootLogger();
		//rootLoggerConfig.getAppenderRefs().add(ref);
		rootLoggerConfig.setLevel(defaultLogLevel);
		//rootLoggerConfig.addAppender(rollingFileAppender, null, null);
		//rootLoggerConfig.addAppender(consoleAppender, defaultLogLevel, null);

		ctx.updateLoggers();
	}

	public void test() {
		restTemplate = new RestTemplate(getRequestFactory());
		List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();
		messageConverters.add(new MappingJackson2HttpMessageConverter());
		restTemplate.setMessageConverters(messageConverters);
		//restTemplate.getInterceptors().add(new LoggingInterceptor());

		Map<String, Object> params = new HashMap<>();
		params.put("accesscode", "3324cfdf-7d3e-4792-bd32-571638d4562f");

		LoginResp loginResp = postOne(params, "users/generatetoken", LoginResp.class);
		System.out.println(loginResp.getId());
	}


	private URI queryString(String url, Map<String, ?> params) {
		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url);

		for (Map.Entry<String, ?> entry : params.entrySet()) {
			builder.queryParam(entry.getKey(), UriUtils.encode(entry.getValue().toString(), StandardCharsets.UTF_8));
		}

		return builder.build(true).toUri();
	}

	public void testFindOne() {
		String url = "http://127.0.0.1:30102/api/Jobs";

		Map<String, String> params = new HashMap<>();
		params.put("access_token", "QB7vQoHux5V6wqqt19ibEfrYAFr6m3ysyhZ0IKFQljjCY7icU6wVG5oRBV5eWiDT");
		params.put("filter", "{ \"where\" : { \"dataFlowId\" : \"605bf29227209634b43089ea\" }, \"fields\" : { \"startupTime\" : true, \"id\" : true, \"name\" : true, \"priority\" : true, \"first_ts\" : true, \"last_ts\" : true, \"user_id\" : true, \"connections\" : true, \"mappings\" : true, \"deployment\" : true, \"mapping_template\" : true, \"status\" : true, \"source\" : true, \"offset\" : true, \"fullSyncSucc\" : true, \"event_job_editted\" : true, \"event_job_error\" : true, \"event_job_started\" : true, \"event_job_stopped\" : true, \"dataFlowId\" : true, \"warning\" : true, \"progressRateStats\" : true, \"stats\" : true, \"connector_ping_time\" : true, \"ping_time\" : true, \"dbhistory\" : true, \"dbhistoryStr\" : true, \"process_offset\" : true, \"is_validate\" : true, \"validate_offset\" : true, \"tableMappings\" : true, \"testTableMappings\" : true, \"syncObjects\" : true, \"keepSchema\" : true, \"transformerConcurrency\" : true, \"processorConcurrency\" : true, \"sync_type\" : true, \"op_filters\" : true, \"running_mode\" : true, \"sampleRate\" : true, \"is_test_write\" : true, \"test_write\" : true, \"is_null_write\" : true, \"lastStatsTimestamp\" : true, \"drop_target\" : true, \"increment\" : true, \"connectorStopped\" : true, \"transformerStopped\" : true, \"needToCreateIndex\" : true, \"notification_window\" : true, \"notification_interval\" : true, \"lastNotificationTimestamp\" : true, \"isCatchUpLag\" : true, \"lagCount\" : true, \"stopWaitintMills\" : true, \"progressFailCount\" : true, \"nextProgressStatTS\" : true, \"trigger_log_remain_time\" : true, \"trigger_start_hour\" : true, \"is_changeStream_mode\" : true, \"readBatchSize\" : true, \"readCdcInterval\" : true, \"stopOnError\" : true, \"row_count\" : true, \"ts\" : true, \"dataQualityTag\" : true, \"executeMode\" : true, \"limit\" : true, \"debugOrder\" : true, \"previousJob\" : true, \"copyManagerOpen\" : true, \"isOpenAutoDDL\" : true, \"runtimeInfo\" : true, \"stages\" : true, \"isSchedule\" : true, \"cronExpression\" : true, \"nextSyncTime\" : true, \"cdcCommitOffsetInterval\" : true, \"includeTables\" : true, \"timingTargetOffsetInterval\" : true, \"reset\" : true, \"isDistribute\" : true, \"process_id\" : true, \"discardDDL\" : true, \"cdcLagWarnSendMailLastTime\" : true, \"distinctWriteType\" : true, \"maxTransactionLength\" : true, \"isSerialMode\" : true, \"connectorErrorEvents\" : true, \"transformerErrorEvents\" : true, \"connectorLastSyncStage\" : true, \"transformerLastSyncStage\" : true, \"cdcFetchSize\" : true, \"milestones\" : true } }");

		URI queryString = queryString(url, params);

		HttpEntity<String> httpEntity = null;

		String cookies = "isAdmin=0;user_id=60596e8d5631e0e6b283c8e7";
		String region = "CIDC-RP-33,CIDC-RP-33-574";

		if (StringUtils.isNotBlank(cookies)) {
			HttpHeaders headers = new HttpHeaders();
			if (StringUtils.isNotBlank(cookies)) {
				headers.add("Cookie", cookies);
			}
			if (StringUtils.isNotBlank(region)) {
				headers.add("jobTags", region);
			}
			httpEntity = new HttpEntity<>(headers);
		}

		ResponseEntity<ResponseBody> responseEntity = restTemplate.exchange(queryString, HttpMethod.GET, httpEntity, ResponseBody.class);

		ResponseBody body = responseEntity.getBody();
		Object data = body.getData();
		System.out.println(data);
	}

	private ClientHttpRequestFactory getRequestFactory() {
		int threshold = 1024;

		PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();
		poolingHttpClientConnectionManager.setMaxTotal(2000);
		poolingHttpClientConnectionManager.setDefaultMaxPerRoute(2000);

		CloseableHttpClient httpClient = HttpClientBuilder.create()
				.disableAutomaticRetries()
				.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
					if (request instanceof HttpEntityEnclosingRequest) {
						HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;

						if (enclosingRequest.getEntity() instanceof ByteArrayEntity) {
							ByteArrayEntity byteArrayEntity = (ByteArrayEntity) enclosingRequest.getEntity();
							long contentLength = byteArrayEntity.getContentLength();
							if (contentLength > threshold) {
								request.addHeader(org.apache.http.HttpHeaders.CONTENT_ENCODING, "gzip");
								enclosingRequest.setEntity(new GzipCompressingEntity(enclosingRequest.getEntity()));
							}
						}

					}
				})
				.setConnectionManager(poolingHttpClientConnectionManager)
				.build();

		HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
		requestFactory.setConnectTimeout(60000);
		requestFactory.setReadTimeout(60000);
		requestFactory.setConnectionRequestTimeout(60000);
		return requestFactory;
	}

	public <T> T postOne(Object obj, String resource, Class<T> className) {
		String baseURL = "http://127.0.0.1:30102/api/";

		try {
			ResponseEntity<ResponseBody> responseEntity = restTemplate.postForEntity(baseURL + resource, obj, ResponseBody.class);

			T result = null;
			if (successResp(responseEntity)) {
				result = getBody(responseEntity.getBody(), className);
			}
			return result;
		} catch (Exception e) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException ignore) {
			}
		}

		return null;
	}


	private <T> T getBody(ResponseBody responseBody, Class<T> className) throws IOException {

		Object data = responseBody.getData();
		if (data != null) {
			return JSONUtil.json2POJO(JSONUtil.obj2Json(data), className);
		}

		return null;
	}

	private boolean successResp(ResponseEntity<ResponseBody> responseEntity) {
		if (responseEntity == null) {
			return false;
		}

		if (!responseEntity.hasBody()) {
			return false;
		}

		return responseEntity.getStatusCode().is2xxSuccessful() && "ok".equals(responseEntity.getBody().getCode());
	}
}
