package com.hazelcast.persistence.http;

import com.tapdata.tm.sdk.available.CloudRestTemplate;
import com.tapdata.tm.sdk.interceptor.VersionHeaderInterceptor;
import com.tapdata.tm.sdk.util.CloudSignUtil;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;

/**
 * 引擎 IT 补丁类：hazelcast-persistence 5.5.0-SNAPSHOT 的 HttpUtil 调用 spring-web 7 已移除的
 * {@code setConnectTimeout(int)}（仓库升级 Spring Boot 4.0.6 的遗留兼容问题），导致 DRS/cloud 模式下
 * 任务状态存储（httptm → HttpResource）初始化时抛 NoSuchMethodError。
 * <p>
 * 本类与 jar 内同名类共存，编译进 test-classes 后优先于依赖 jar 加载，行为等价：
 * 仅将 {@code setConnectTimeout(int)} 替换为 spring-web 7 提供的 {@code setConnectionRequestTimeout(int)}。
 */
public class HttpUtil {

	public HttpUtil() {
	}

	public static RestTemplate getRestTemplate(int connectTimeout, int readTimeout) {
		RestTemplate restTemplate;
		if (CloudSignUtil.isNeedSign()) {
			restTemplate = new CloudRestTemplate(getClientHttpRequestFactory(connectTimeout, readTimeout));
		} else {
			restTemplate = new RestTemplate(getClientHttpRequestFactory(connectTimeout, readTimeout));
		}
		restTemplate.getMessageConverters().add(new TxMappingJackson2HttpMessageConverter());
		restTemplate.getInterceptors().add(new VersionHeaderInterceptor());
		return restTemplate;
	}

	private static ClientHttpRequestFactory getClientHttpRequestFactory(int connectTimeout, int readTimeout) {
		try {
			TrustStrategy trustStrategy = (chain, authType) -> true;
			SSLContext sslContext = SSLContextBuilder.create()
					.loadTrustMaterial(null, trustStrategy)
					.build();
			PoolingHttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder.create()
					.setTlsSocketStrategy(new DefaultClientTlsStrategy(sslContext, new NoopHostnameVerifier()))
					.build();
			RequestConfig requestConfig = RequestConfig.custom()
					.setCookieSpec("default")
					.build();
			CloseableHttpClient httpClient = HttpClients.custom()
					.setDefaultRequestConfig(requestConfig)
					.disableAutomaticRetries()
					.setConnectionManager(connectionManager)
					.build();
			HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
			factory.setHttpClient(httpClient);
			// spring-web 7 移除了 setConnectTimeout(int)，以 setConnectionRequestTimeout(int) 近似替代
			factory.setConnectionRequestTimeout(connectTimeout > 0 ? connectTimeout : 10000);
			factory.setReadTimeout(readTimeout > 0 ? readTimeout : 30000);
			return factory;
		} catch (Exception e) {
			throw new RuntimeException(String.format("Create http request factory failed, message: %s", e.getMessage()), e);
		}
	}

	/** 与 jar 内同名内部类保持一致：额外支持 text/plain + text/html 的 JSON 消息转换器 */
	static class TxMappingJackson2HttpMessageConverter extends MappingJackson2HttpMessageConverter {
		public TxMappingJackson2HttpMessageConverter() {
			List<MediaType> supported = new ArrayList<>();
			supported.add(MediaType.TEXT_PLAIN);
			supported.add(MediaType.TEXT_HTML);
			setSupportedMediaTypes(supported);
		}
	}
}
