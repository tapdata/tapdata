package com.tapdata.processor.util;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomRest {

	private final static String RESULT_CODE = "code";
	private final static String RESULT_DATA = "data";
	private final static String RESULT_ERR_MESSAGE = "err_message";
	private final static int CONNECT_TIMEOUT = 10_000;
	private final static int READ_TIMEOUT = 30_000;
	private final static String ASC = "asc";
	private final static String DESC = "desc";
	private final static String RETURN_TYPE_ARRAY = "array";
	private final static String RETURN_TYPE_OBJECT = "object";
	private final static String RETURN_TYPE_STRING = "string";

	/**
	 * 默认的header，key必须小写，会自动与传入的header合并，如果key冲突，优先采用传入的header
	 */
	private final static Map<String, Object> DEFAULT_HEADER = new HashMap<String, Object>() {{
		put("content_type", "application/json");
	}};

	public static Map<String, Object> get(String url) {
		return get(url, new HashMap<>(), RETURN_TYPE_ARRAY);
	}

	public static Map<String, Object> get(String url, int connectTimeout, int readTimeout) {
		return get(url, new HashMap<>(), RETURN_TYPE_ARRAY, connectTimeout, readTimeout);
	}

	public static Map<String, Object> get(String url, String returnType) {
		return get(url, new HashMap<>(), returnType);
	}

	public static Map<String, Object> get(String url, String returnType, int connectTimeout, int readTimeout) {
		return get(url, new HashMap<>(), returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> get(String url, Map<String, Object> headersMap) {
		return get(url, headersMap, RETURN_TYPE_ARRAY);
	}

	public static Map<String, Object> get(String url, Map<String, Object> headersMap, int connectTimeout, int readTimeout) {
		return get(url, headersMap, RETURN_TYPE_ARRAY, connectTimeout, readTimeout);
	}

	public static Map<String, Object> get(String url, Map<String, Object> headersMap, String returnType) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.GET.name(), mergeHeader(headers), new HashMap<>());

		return requestData(url, HttpMethod.GET, httpEntity, returnType, CONNECT_TIMEOUT, READ_TIMEOUT);
	}

	public static Map<String, Object> get(String url, Map<String, Object> headersMap, String returnType, int connectTimeout, int readTimeout) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.GET.name(), mergeHeader(headers), new HashMap<>());

		return requestData(url, HttpMethod.GET, httpEntity, returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> post(String url) {
		return post(url, new HashMap<>(), new HashMap<>(), RETURN_TYPE_ARRAY);
	}

	public static Map<String, Object> post(String url, int connectTimeout, int readTimeout) {
		return post(url, new HashMap<>(), new HashMap<>(), RETURN_TYPE_ARRAY, connectTimeout, readTimeout);
	}

	public static Map<String, Object> post(String url, String returnType) {
		return post(url, new HashMap<>(), new HashMap<>(), returnType);
	}

	public static Map<String, Object> post(String url, String returnType, int connectTimeout, int readTimeout) {
		return post(url, new HashMap<>(), new HashMap<>(), returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> post(String url, String paramsStr, String returnType) {
		Map<String, Object> paramsMap = new HashMap<>();
		try {
			Arrays.stream(paramsStr.split("\n")).forEach(line -> paramsMap.put(line.substring(0, line.indexOf("=")), line.substring(line.indexOf("=") + 1)));
		} catch (Exception e) {
			throw new IllegalArgumentException("params invalid");
		}
		return post(url, paramsMap, new HashMap<>(), returnType);
	}

	public static Map<String, Object> post(String url, String paramsStr, String returnType, int connectTimeout, int readTimeout) {
		Map<String, Object> paramsMap = new HashMap<>();
		try {
			Arrays.stream(paramsStr.split("\n")).forEach(line -> paramsMap.put(line.substring(0, line.indexOf("=")), line.substring(line.indexOf("=") + 1)));
		} catch (Exception e) {
			throw new IllegalArgumentException("params invalid");
		}
		return post(url, paramsMap, new HashMap<>(), returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> post(String url, Map<String, Object> paramsMap) {
		return post(url, paramsMap, new HashMap<>(), RETURN_TYPE_ARRAY);
	}

	public static Map<String, Object> post(String url, Map<String, Object> paramsMap, int connectTimeout, int readTimeout) {
		return post(url, paramsMap, new HashMap<>(), RETURN_TYPE_ARRAY, connectTimeout, readTimeout);
	}

	public static Map<String, Object> post(String url, Map<String, Object> paramsMap, String returnType) {
		return post(url, paramsMap, new HashMap<>(), returnType);
	}

	public static Map<String, Object> post(String url, Map<String, Object> paramsMap, String returnType, int connectTimeout, int readTimeout) {
		return post(url, paramsMap, new HashMap<>(), returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> post(String url, Map<String, Object> paramsMap, Map<String, Object> headersMap, String returnType) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		Map<String, Object> params = new HashMap<>();
		MapUtil.copyToNewMap(paramsMap, params);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.POST.name(), mergeHeader(headers), params);

		return requestData(url, HttpMethod.POST, httpEntity, returnType, CONNECT_TIMEOUT, READ_TIMEOUT);
	}

	public static Map<String, Object> post(String url, Map<String, Object> paramsMap, Map<String, Object> headersMap, String returnType,
										   int connectTimeout, int readTimeout) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		Map<String, Object> params = new HashMap<>();
		MapUtil.copyToNewMap(paramsMap, params);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.POST.name(), mergeHeader(headers), params);

		return requestData(url, HttpMethod.POST, httpEntity, returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> post(String url, byte[] params, MultiValueMap<String, String> headersMap, String returnType) {
		MultiValueMap<String, String> headers = new HttpHeaders();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity<byte[]> httpEntity = new HttpEntity<>(params, headers);

		return requestData(url, HttpMethod.POST, httpEntity, returnType, CONNECT_TIMEOUT, READ_TIMEOUT);
	}

	public static Map<String, Object> post(String url, byte[] params, MultiValueMap<String, String> headersMap, String returnType,
										   int connectTimeout, int readTimeout) {
		MultiValueMap<String, String> headers = new HttpHeaders();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity<byte[]> httpEntity = new HttpEntity<>(params, headers);

		return requestData(url, HttpMethod.POST, httpEntity, returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> put(String url) {
		return put(url, new HashMap<>(), new HashMap<>(), RETURN_TYPE_ARRAY);
	}

	public static Map<String, Object> put(String url, int connectTimeout, int readTimeout) {
		return put(url, new HashMap<>(), new HashMap<>(), RETURN_TYPE_ARRAY, connectTimeout, readTimeout);
	}

	public static Map<String, Object> put(String url, String returnType) {
		return put(url, new HashMap<>(), new HashMap<>(), returnType);
	}

	public static Map<String, Object> put(String url, String returnType, int connectTimeout, int readTimeout) {
		return put(url, new HashMap<>(), new HashMap<>(), returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> put(String url, String paramsStr, String returnType) {
		Map<String, Object> paramsMap = new HashMap<>();
		try {
			Arrays.stream(paramsStr.split("\n")).forEach(line -> paramsMap.put(line.substring(0, line.indexOf("=")), line.substring(line.indexOf("=") + 1)));
		} catch (Exception e) {
			throw new IllegalArgumentException("params invalid");
		}
		return put(url, paramsMap, new HashMap<>(), returnType);
	}

	public static Map<String, Object> put(String url, String paramsStr, String returnType, int connectTimeout, int readTimeout) {
		Map<String, Object> paramsMap = new HashMap<>();
		try {
			Arrays.stream(paramsStr.split("\n")).forEach(line -> paramsMap.put(line.substring(0, line.indexOf("=")), line.substring(line.indexOf("=") + 1)));
		} catch (Exception e) {
			throw new IllegalArgumentException("params invalid");
		}
		return put(url, paramsMap, new HashMap<>(), returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> put(String url, Map<String, Object> paramsMap) {
		return put(url, paramsMap, new HashMap<>(), RETURN_TYPE_ARRAY);
	}

	public static Map<String, Object> put(String url, Map<String, Object> paramsMap, int connectTimeout, int readTimeout) {
		return put(url, paramsMap, new HashMap<>(), RETURN_TYPE_ARRAY, connectTimeout, readTimeout);
	}

	public static Map<String, Object> put(String url, Map<String, Object> paramsMap, String returnType) {
		return put(url, paramsMap, new HashMap<>(), returnType);
	}

	public static Map<String, Object> put(String url, Map<String, Object> paramsMap, String returnType, int connectTimeout, int readTimeout) {
		return put(url, paramsMap, new HashMap<>(), returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> put(String url, Map<String, Object> paramsMap, Map<String, Object> headersMap, String returnType) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		Map<String, Object> params = new HashMap<>();
		MapUtil.copyToNewMap(paramsMap, params);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.PUT.name(), mergeHeader(headers), params);

		return requestData(url, HttpMethod.PUT, httpEntity, returnType, CONNECT_TIMEOUT, READ_TIMEOUT);
	}

	public static Map<String, Object> put(String url, Map<String, Object> paramsMap, Map<String, Object> headersMap, String returnType,
										  int connectTimeout, int readTimeout) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		Map<String, Object> params = new HashMap<>();
		MapUtil.copyToNewMap(paramsMap, params);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.PUT.name(), mergeHeader(headers), params);

		return requestData(url, HttpMethod.PUT, httpEntity, returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> put(String url, byte[] params, MultiValueMap<String, String> headersMap, String returnType) {
		MultiValueMap<String, String> headers = new HttpHeaders();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity<byte[]> httpEntity = new HttpEntity<>(params, headers);

		return requestData(url, HttpMethod.PUT, httpEntity, returnType, CONNECT_TIMEOUT, READ_TIMEOUT);
	}

	public static Map<String, Object> put(String url, byte[] params, MultiValueMap<String, String> headersMap, String returnType,
										  int connectTimeout, int readTimeout) {
		MultiValueMap<String, String> headers = new HttpHeaders();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity<byte[]> httpEntity = new HttpEntity<>(params, headers);

		return requestData(url, HttpMethod.PUT, httpEntity, returnType, connectTimeout, readTimeout);
	}

	public static Map<String, Object> patch(String url, Map<String, Object> paramsMap) {
		return patch(url, paramsMap, new HashMap<>());
	}

	public static Map<String, Object> patch(String url, Map<String, Object> paramsMap, int connectTimeout, int readTimeout) {
		return patch(url, paramsMap, new HashMap<>(), connectTimeout, readTimeout);
	}

	public static Map<String, Object> patch(String url, Map<String, Object> paramsMap, Map<String, Object> headersMap) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		Map<String, Object> params = new HashMap<>();
		MapUtil.copyToNewMap(paramsMap, params);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.PATCH.name(), mergeHeader(headers), params);

		return requestData(url, HttpMethod.PATCH, httpEntity, RETURN_TYPE_OBJECT, CONNECT_TIMEOUT, READ_TIMEOUT);
	}

	public static Map<String, Object> patch(String url, Map<String, Object> paramsMap, Map<String, Object> headersMap,
											int connectTimeout, int readTimeout) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		Map<String, Object> params = new HashMap<>();
		MapUtil.copyToNewMap(paramsMap, params);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.PATCH.name(), mergeHeader(headers), params);

		return requestData(url, HttpMethod.PATCH, httpEntity, RETURN_TYPE_OBJECT, connectTimeout, readTimeout);
	}

	public static Map<String, Object> delete(String url) {
		return delete(url, new HashMap<>());
	}

	public static Map<String, Object> delete(String url, int connectTimeout, int readTimeout) {
		return delete(url, new HashMap<>(), connectTimeout, readTimeout);
	}

	public static Map<String, Object> delete(String url, Map<String, Object> headersMap) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.DELETE.name(), mergeHeader(headers), new HashMap<>());

		return requestData(url, HttpMethod.DELETE, httpEntity, RETURN_TYPE_OBJECT, CONNECT_TIMEOUT, READ_TIMEOUT);
	}

	public static Map<String, Object> delete(String url, Map<String, Object> headersMap,
											 int connectTimeout, int readTimeout) {
		Map<String, Object> headers = new HashMap<>();
		MapUtil.copyToNewMap(headersMap, headers);
		HttpEntity httpEntity = RestUtil.httpEntity(HttpMethod.DELETE.name(), mergeHeader(headers), new HashMap<>());

		return requestData(url, HttpMethod.DELETE, httpEntity, RETURN_TYPE_OBJECT, connectTimeout, readTimeout);
	}

	public static String sort(Map<String, Object> inMap) {
		return asciiSort(inMap, ASC);
	}

	public static String asciiSort(Map<String, Object> inMap, String sort) {
		StringBuilder stringBuilder = new StringBuilder();

		if (MapUtils.isNotEmpty(inMap)) {
			Map<String, Object> map = recursiveMap(inMap, null);
			List<Map.Entry<String, Object>> sortList = new ArrayList<>(map.entrySet());

			Collections.sort(sortList, (o1, o2) -> {
				if (sort.equals(ASC)) {
					return o1.getKey().compareTo(o2.getKey());
				} else {
					return o2.getKey().compareTo(o1.getKey());
				}
			});

			for (Map.Entry<String, Object> entry : sortList) {
				stringBuilder.append(entry.getKey())
						.append("=")
						.append(entry.getValue())
						.append("&");
			}
		}

		String returnStr = stringBuilder.toString();
		if (StringUtils.endsWithIgnoreCase(returnStr, "&")) {
			return StringUtils.removeEnd(returnStr, "&");
		} else {
			return returnStr;
		}
	}

	public static Map<String, Object> recursiveMap(Map<String, Object> map, String pKey) {
		Map<String, Object> returnMap = new HashMap<>();
		if (MapUtils.isNotEmpty(map)) {
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				String k = entry.getKey();
				Object v = entry.getValue();

				if (v instanceof Map) {
					Map<String, Object> tempMap = recursiveMap((Map<String, Object>) v, k);
					returnMap.putAll(tempMap);
				} else if (v instanceof List) {
					Map<String, Object> tempMap = recursiveListMap((List) v, k);
					returnMap.putAll(tempMap);
				} else {
					if (StringUtils.isNotBlank(pKey)) {
						returnMap.put(pKey + "[" + k + "]", v);
					} else {
						returnMap.put(k, v);
					}
				}
			}
		}

		return returnMap;
	}

	public static Map<String, Object> recursiveListMap(List<Object> list, String pKey) {
		Map<String, Object> returnMap = new HashMap<>();
		if (CollectionUtils.isNotEmpty(list)) {
			for (int i = 0; i < list.size(); i++) {
				Object object = list.get(i);
				if (object instanceof Map) {
					if (StringUtils.isNotBlank(pKey)) {
						Map<String, Object> tempMap = recursiveMap((Map) object, pKey + "[" + i + "]");
						returnMap.putAll(tempMap);
					} else {
						returnMap.put("[" + i + "]", object);
					}
				} else {
					returnMap.put(pKey + "[" + i + "]", object);
				}
			}
		}
		return returnMap;
	}

	private static Map<String, Object> requestData(String url, HttpMethod httpMethod, HttpEntity httpEntity, String returnType, int connectTimeout, int readTimeout) {

		Map<String, Object> result = new HashMap<>();

		try {
			ResponseEntity responseEntity = null;

			URI reqURI = URI.create(url);
			if (RETURN_TYPE_OBJECT.equalsIgnoreCase(returnType)) {
				try {
					responseEntity = getRestTemplate(connectTimeout, readTimeout).exchange(reqURI, httpMethod, httpEntity, Object.class);
				} catch (RestClientException e) {
					responseEntity = getRestTemplate(connectTimeout, readTimeout).exchange(reqURI, httpMethod, httpEntity, List.class);
				}
			} else if (RETURN_TYPE_STRING.equalsIgnoreCase(returnType)) {
				responseEntity = getRestTemplate(connectTimeout, readTimeout).exchange(reqURI, httpMethod, httpEntity, String.class);
			} else {
				try {
					responseEntity = getRestTemplate(connectTimeout, readTimeout).exchange(reqURI, httpMethod, httpEntity, List.class);
				} catch (RestClientException e) {
					responseEntity = getRestTemplate(connectTimeout, readTimeout).exchange(reqURI, httpMethod, httpEntity, Object.class);
				}
			}

			if (responseEntity == null) {
				result.put(RESULT_CODE, HttpStatus.BAD_REQUEST.value());
				result.put(RESULT_ERR_MESSAGE, "response is null");
				result.put(RESULT_DATA, new HashMap<String, String>() {{
					put("err", "response is null");
				}});
			} else {
				result.put(RESULT_CODE, responseEntity.getStatusCodeValue());
				result.put(RESULT_DATA, responseEntity.getBody());
			}
		} catch (RestClientException e) {
			String stackString = Log4jUtil.getStackString(e);
			result.put(RESULT_CODE, HttpStatus.BAD_REQUEST.value());
			result.put(RESULT_ERR_MESSAGE, "request error: " + e.getMessage());
			result.put(RESULT_DATA, new HashMap<String, Object>() {{
				put("err", String.format("unknown error: %s", e.getMessage()));
				put("stack", stackString);
			}});
		}

		return result;
	}

	private static RestTemplate getRestTemplate(int connectTimeout, int readTimeout) {
		RestTemplate restTemplate = new RestTemplate(getClientHttpRequestFactory(connectTimeout, readTimeout));
		restTemplate.getMessageConverters().add(new TxMappingJackson2HttpMessageConverter());

		return restTemplate;
	}

	static class TxMappingJackson2HttpMessageConverter extends MappingJackson2HttpMessageConverter {
		public TxMappingJackson2HttpMessageConverter() {
			List<MediaType> mediaTypes = new ArrayList<>();
			mediaTypes.add(MediaType.TEXT_PLAIN);
			mediaTypes.add(MediaType.TEXT_HTML);
			setSupportedMediaTypes(mediaTypes);
		}
	}

	private static ClientHttpRequestFactory getClientHttpRequestFactory(int connectTimeout, int readTimeout) {
		HttpComponentsClientHttpRequestFactory factory;
		try {
			TrustStrategy acceptingTrustStrategy = (x509Certificates, authType) -> true;
			SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
			SSLConnectionSocketFactory connectionSocketFactory =
					new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());

			HttpClientBuilder httpClientBuilder = HttpClients.custom();
			httpClientBuilder.setSSLSocketFactory(connectionSocketFactory);
			CloseableHttpClient httpClient = httpClientBuilder.build();
			factory = new HttpComponentsClientHttpRequestFactory();
			factory.setHttpClient(httpClient);

			//Connect timeout
			factory.setConnectTimeout(connectTimeout <= 0 ? CONNECT_TIMEOUT : connectTimeout);

			//Read timeout
			factory.setReadTimeout(readTimeout <= 0 ? READ_TIMEOUT : readTimeout);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Create http request factory failed, message: %s, stacks: %s", e.getMessage(),
					Log4jUtil.getStackString(e)));
		}
		return factory;
	}

	private static Map<String, Object> mergeHeader(Map<String, Object> header) {
		if (header == null) {
			header = new HashMap<>();
		}

		Map<String, Object> finalHeader = header;
		DEFAULT_HEADER.keySet().stream().forEach(key -> {
			if (!finalHeader.containsKey(key)) {
				finalHeader.put(key, DEFAULT_HEADER.get(key));
			}
		});

		return header;
	}
}
