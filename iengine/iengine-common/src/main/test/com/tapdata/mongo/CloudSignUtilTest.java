package com.tapdata.mongo;

import com.tapdata.entity.LoginResp;
import com.tapdata.entity.ResponseBody;
import com.tapdata.tm.sdk.util.SignUtil;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.jdbc.core.metadata.HanaCallMetaDataProvider;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class CloudSignUtilTest {

	public static void main(String[] args) throws UnsupportedEncodingException, URISyntaxException {
//    CloudRestTemplate cloudRestTemplate = new CloudRestTemplate(getRequestFactory(60000, 60000, 60000));
//    String url = "http://127.0.0.1:9999/test/hello?accessKey=gJXFNomvIlsrrCt9l4jvhVFu1Vm9iXyQ&signVersion=1.0&sign=neYd8JEd27m4JqhzsrNAzvq%2F0vc%3D&nonce=e73c3c52-6129-4daa-b178-51e9ae2c53cb&ts=1628479028057";
//    Map<String, Object> param = new HashMap<>();
//    param.put("accesscode", "d1c7fac35971765f9ec7683c2d28f816");
//    cloudRestTemplate.postForEntity(url, param, ResponseBody.class);

//    String x = SignUtil.percentEncode("oH1hoFtJL3e+EznmOCRJa4Dltoo=");
//    System.out.println(x);
//    System.out.println(SignUtil.percentEncode(x));

		URI uri = new URI("http://192.168.1.189:8086/tm/api/users/generatetoken?accessKey=gJXFNomvIlsrrCt9l4jvhVFu1Vm9iXyQ&signVersion=1.0&sign=qEzq2iYHjBLUtbibeCehSIfYaJA%3D&nonce=46d6cbcb-60ca-4a1f-bbdc-8f262f7c3e79&ts=1628490043715");
		System.out.println(uri.toString());
	}

	private static ClientHttpRequestFactory getRequestFactory(int connectTimeout, int readTimeout, int connectRequestTimeout) {
		int threshold = 1024;

		PoolingHttpClientConnectionManager poolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager();
		poolingHttpClientConnectionManager.setMaxTotal(2000);
		poolingHttpClientConnectionManager.setDefaultMaxPerRoute(2000);

		CloseableHttpClient httpClient = HttpClientBuilder.create()
				.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
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
		requestFactory.setConnectTimeout(connectTimeout);
		requestFactory.setReadTimeout(readTimeout);
		requestFactory.setConnectionRequestTimeout(connectRequestTimeout);
		return requestFactory;
	}
}
