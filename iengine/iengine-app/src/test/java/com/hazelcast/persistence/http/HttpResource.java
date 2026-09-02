package com.hazelcast.persistence.http;

import com.hazelcast.persistence.config.PersistenceHttpConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 引擎 IT 补丁类：hazelcast-persistence 5.5.0-SNAPSHOT 的 HttpResource 编译于 spring 6 时代，
 * 其 {@code retryWrap} 调用 spring-web 7 已移除的 {@code HttpClientErrorException.getRawStatusCode()}，
 * 导致 DRS/cloud 模式下任务状态存储（httptm → HttpResource）初始化时抛 NoSuchMethodError。
 * <p>
 * 本类与 jar 内同名类共存，编译进 test-classes 后优先于依赖 jar 加载，行为等价：
 * 仅将 {@code getRawStatusCode()} 替换为 spring-web 7 的 {@code getStatusCode().value()}；
 * {@code org.apache.http.NoHttpResponseException} 通过反射判断，避免引入 httpcore 4 编译依赖。
 */
public class HttpResource extends ExternalResource<PersistenceHttpConfig> {

	private final Logger logger = LogManager.getLogger(HttpResource.class);

	private List<String> baseURLs;

	private String baseUrl;

	private int connectTimeout;

	private int readTimeout;

	private RestTemplate restTemplate;

	private Supplier<Long> getRetryTimeout;

	private int retryTime = 10;

	private long retryInterval = 500;

	private static final String ERR_MSG_FORMAT = "Failed to call rest api, msg %s.";

	/** NoHttpResponseException（httpcore 4）可能不在 classpath，反射判断以保持等价行为 */
	private static final Class<?> NO_HTTP_RESPONSE_EXCEPTION;

	static {
		Class<?> clazz = null;
		try {
			clazz = Class.forName("org.apache.http.NoHttpResponseException");
		} catch (ClassNotFoundException ignored) {
		}
		NO_HTTP_RESPONSE_EXCEPTION = clazz;
	}

	public HttpResource() {
	}

	public void doInit(PersistenceHttpConfig config) {
		baseURLs = config.getBaseURLs();
		if (baseURLs == null || baseURLs.size() == 0) {
			throw new IllegalArgumentException("Base url cannot be empty");
		}
		baseUrl = baseURLs.get(0);
		Object connectTimeoutObj = config.getConnectTimeoutMs();
		if (connectTimeoutObj instanceof String) {
			connectTimeout = Integer.parseInt(connectTimeoutObj.toString());
		}
		Object readTimeoutObj = config.getReadTimeoutMs();
		if (readTimeoutObj instanceof String) {
			readTimeout = Integer.parseInt(readTimeoutObj.toString());
		}
		restTemplate = HttpUtil.getRestTemplate(connectTimeout, readTimeout);
		getRetryTimeout = () -> 30000L;
	}

	public List<String> getBaseURLs() {
		return baseURLs;
	}

	public String getBaseUrl() {
		return baseUrl;
	}

	public RestTemplate getRestTemplate() {
		return restTemplate;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public int getReadTimeout() {
		return readTimeout;
	}

	public void close() {
	}

	public <T> T retryWrap(HttpResource.TryFunc<T> func, Predicate<?> predicate) {
		long timeout = Optional.ofNullable(getRetryTimeout)
				.map(Supplier::get)
				.orElse((long) retryTime * retryInterval);
		HttpResource.RetryInfo retryInfo = new HttpResource.RetryInfo(baseUrl, timeout);
		while (true) {
			try {
				T result = func.tryFunc(retryInfo);
				if (retryInfo.lastError != null) {
					logger.info("RestApi '{}' completed, use {}ms, retries {}",
							retryInfo.reqId, System.currentTimeMillis() - retryInfo.begin, retryInfo.retries);
					baseUrl = retryInfo.baseURL;
				}
				return result;
			} catch (HttpMessageConversionException | InterruptedException e) {
				// 与 jar 版一致：这两类异常不做重试，直接走到统一错误出口
				break;
			} catch (Exception e) {
				if (e instanceof HttpClientErrorException) {
					// spring-web 7 移除 getRawStatusCode()，改用 getStatusCode().value()
					int status = ((HttpClientErrorException) e).getStatusCode().value();
					if (status == 404) {
						throw new RuntimeException(String.format(ERR_MSG_FORMAT, retryInfo.reqURL), e);
					}
					if (status == 405) {
						throw new RuntimeException(String.format(ERR_MSG_FORMAT, "Please upgrade engine"), e);
					}
				}
				boolean isRetriable = true;
				Throwable cause = e;
				while (cause != null) {
					if (NO_HTTP_RESPONSE_EXCEPTION != null && NO_HTTP_RESPONSE_EXCEPTION.isInstance(cause)) {
						isRetriable = false;
						break;
					}
					cause = cause.getCause();
				}
				if (retryInfo.lastError == null) {
					logger.warn("RestApi '{}' failed, use {}ms, retryTime {}ms, retryInterval {}ms, reqURL: {}, reqParams: {}, error message: {}",
							retryInfo.reqId, System.currentTimeMillis() - retryInfo.begin, retryInfo.timeout, retryInterval,
							retryInfo.reqURL, retryInfo.reqParams, e.getMessage());
				}
				try {
					TimeUnit.MILLISECONDS.sleep(retryInterval);
				} catch (InterruptedException ie) {
					break;
				}
				retryInfo.retries++;
				if (isRetriable) {
					retryInfo.lastError = e;
					retryInfo.baseURL = changeBaseURLToNext(retryInfo.baseURL);
				}
				if (System.currentTimeMillis() - retryInfo.begin < retryInfo.timeout
						&& (predicate == null || predicate.test(null))) {
					continue;
				}
				break;
			}
		}
		if (retryInfo.lastError == null) {
			throw new RuntimeException(String.format(ERR_MSG_FORMAT, "no exception"));
		}
		if (retryInfo.reqParams != null) {
			throw new RuntimeException(String.format(ERR_MSG_FORMAT,
					retryInfo.reqParams.toString().getBytes().length / 1024 / 1024
							+ " MB, error: " + retryInfo.lastError.getMessage()), retryInfo.lastError);
		}
		throw new RuntimeException(String.format(ERR_MSG_FORMAT, retryInfo.lastError.getMessage()), retryInfo.lastError);
	}

	private synchronized String changeBaseURLToNext(String baseUrl) {
		int nextIndex = 0;
		for (int i = 0; i < baseURLs.size() - 1; i++) {
			if (baseUrl.equals(baseURLs.get(i))) {
				nextIndex = i + 1;
				break;
			}
		}
		return baseURLs.get(nextIndex);
	}

	interface TryFunc<T> {
		T tryFunc(HttpResource.RetryInfo retryInfo) throws Exception;
	}

	static class RetryInfo {
		private final String reqId;
		private final long begin;
		private final long timeout;
		private long retries;
		private String baseURL;
		private String reqURL;
		private Object reqParams;
		private Exception lastError;

		public RetryInfo(String baseURL, long timeout) {
			this.baseURL = baseURL;
			this.timeout = timeout;
			this.begin = System.currentTimeMillis();
			this.reqId = java.util.UUID.randomUUID().toString();
		}

		void showParams(Object reqParams) {
			this.reqParams = reqParams;
		}

		String getURL(String url) {
			this.reqURL = baseURL + url;
			return reqURL;
		}

		protected java.net.URI getURI(String... paths) {
			return getURI(null, paths);
		}

		protected java.net.URI getURI(java.util.Map<String, ?> params, String... paths) {
			StringBuilder sb = new StringBuilder(baseURL);
			if (paths != null) {
				for (String path : paths) {
					sb.append("/").append(path);
				}
			}
			org.springframework.web.util.UriComponentsBuilder builder =
					org.springframework.web.util.UriComponentsBuilder.fromUriString(sb.toString());
			if (org.apache.commons.collections4.MapUtils.isNotEmpty(params)) {
				for (java.util.Map.Entry<String, ?> entry : params.entrySet()) {
					builder.queryParam(entry.getKey(),
							org.springframework.web.util.UriUtils.encode(String.valueOf(entry.getValue()), java.nio.charset.StandardCharsets.UTF_8));
				}
			}
			return builder.build(true).toUri();
		}
	}
}
