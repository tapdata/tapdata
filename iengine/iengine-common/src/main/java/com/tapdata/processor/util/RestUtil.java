package com.tapdata.processor.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.RestURLInfo;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RestUtil {

	private static Logger logger = LogManager.getLogger(RestUtil.class);

	public static final String TOKEN_BUILD_IN_PARAM = "${access_token}";
	public static final String PAGE_NUMBER_BUILD_IN_PARAM = "${page_number}";
	public static final String OFFSET_BUILD_IN_PARAM = "${offset}";

	public static RestRequestEntity adapteRequestEntity(RestURLInfo urlInfo, RestURLInfo tokenURLInfo) throws UnsupportedEncodingException, MalformedURLException {
		String url = urlInfo.getUrl();
		Map<String, Object> headers = urlInfo.getHeaders();
		Map<String, Object> requestParameters = urlInfo.getRequest_parameters();
		HttpMethod httpMethod = HttpMethod.resolve(urlInfo.getMethod());
		if (HttpMethod.POST == httpMethod) {
			Map<String, String> urlParams = splitQuery(url);
			if (requestParameters == null) {
				requestParameters = new HashMap<>();
			}
			if (MapUtils.isNotEmpty(urlParams)) {
				requestParameters.putAll(urlParams);
			}
		}
		HttpEntity httpEntity = httpEntity(urlInfo.getMethod(), headers, requestParameters);
		return new RestRequestEntity(httpEntity, httpMethod, url, tokenURLInfo);
	}

	public static HttpHeaders httpHeaders(Map<String, Object> headers) {
		HttpHeaders httpHeaders = null;
		if (MapUtils.isNotEmpty(headers)) {
			httpHeaders = new HttpHeaders();
			for (Map.Entry<String, Object> entry : headers.entrySet()) {
				httpHeaders.add(entry.getKey().toLowerCase(), entry.getValue().toString());
			}
		}
		return httpHeaders;
	}

	public static HttpEntity httpEntity(String method, Map<String, Object> headers, Map<String, Object> requestParams) {

		HttpEntity httpEntity = null;
		HttpHeaders httpHeaders = httpHeaders(headers);
		if (HttpMethod.GET == HttpMethod.resolve(method)) {
			httpEntity = new HttpEntity(httpHeaders);
		} else {
			MultiValueMap<String, Object> multiValueMap = new LinkedMultiValueMap<>();
			String contentType = headers.getOrDefault("content_type", "") + "";
			if ("application/x-www-form-urlencoded".equalsIgnoreCase(contentType)) {
				requestParams.forEach((k, v) -> {
					if (v instanceof Map || v instanceof List) {
						try {
							multiValueMap.add(k, JSONUtil.obj2Json(v));
						} catch (JsonProcessingException e) {
							logger.error("Build rest http entity error, key: {}, value: {}, message: {}, will skip it", k, v, e.getMessage(), e);
						}
					} else {
						multiValueMap.add(k, v.toString());
					}
				});
				httpEntity = new HttpEntity(multiValueMap, httpHeaders);
			} else {
				httpEntity = new HttpEntity(requestParams, httpHeaders);
			}
		}

		return httpEntity;
	}

	public static RestURLInfo setBuildInParams(RestURLInfo urlInfo, String token, int pageNum, String offset) {

		RestURLInfo restURLInfo = new RestURLInfo(urlInfo);

		Map<String, Object> headers = restURLInfo.getHeaders();
		Map<String, Object> requestParams = restURLInfo.getRequest_parameters();
		String url = restURLInfo.getUrl();

		HttpMethod httpMethod = HttpMethod.resolve(urlInfo.getMethod());
		if (HttpMethod.GET == httpMethod) {
			url = RestUtil.urlQueryString(url, requestParams);
		}

		if (MapUtils.isNotEmpty(headers)) {
			replaceParams(headers, token, pageNum, offset);
		}

		if (MapUtils.isNotEmpty(requestParams)) {
			replaceParams(requestParams, token, pageNum, offset);
		}

		url = StringUtils.isNotBlank(token) ? url.replace(RestUtil.TOKEN_BUILD_IN_PARAM, token) : url;
		url = StringUtils.isNotBlank(offset) ? url.replace(RestUtil.OFFSET_BUILD_IN_PARAM, offset) : url;
		url = pageNum > 0 ? url.replace(RestUtil.PAGE_NUMBER_BUILD_IN_PARAM, String.valueOf(pageNum)) : url;

		restURLInfo.setHeaders(headers);
		restURLInfo.setRequest_parameters(requestParams);
		restURLInfo.setUrl(url);
		return restURLInfo;
	}

	public static boolean isPagination(RestURLInfo urlInfo) {
		Map<String, Object> requestParameters = urlInfo.getRequest_parameters();
		String url = urlInfo.getUrl();
		Map<String, Object> headers = urlInfo.getHeaders();

		if (MapUtils.isNotEmpty(requestParameters)) {
			for (Object value : requestParameters.values()) {
				if (PAGE_NUMBER_BUILD_IN_PARAM.equals(value)) {
					return true;
				}
			}
		}

		if (MapUtils.isNotEmpty(headers)) {
			for (Object value : headers.values()) {
				if (PAGE_NUMBER_BUILD_IN_PARAM.equals(value)) {
					return true;
				}
			}
		}

		return url.contains(PAGE_NUMBER_BUILD_IN_PARAM);
	}

	private static void replaceParams(Map<String, Object> requestParams, String token, int pageNum, String offset) {
		for (Map.Entry<String, Object> entry : requestParams.entrySet()) {

			if (StringUtils.isNotBlank(token)) {
				if (TOKEN_BUILD_IN_PARAM.equals(entry.getValue())) {
					entry.setValue(token);
				}
			}

			if (StringUtils.isNotBlank(offset)) {
				if (OFFSET_BUILD_IN_PARAM.equals(entry.getValue())) {
					entry.setValue(offset);
				}
			}

			if (pageNum > 0) {
				if (PAGE_NUMBER_BUILD_IN_PARAM.equals(entry.getValue())) {
					entry.setValue(String.valueOf(pageNum));
				}
			}
		}
	}


	public static String urlQueryString(String urlString, Map<String, Object> requestParams) {
		StringBuilder sb = new StringBuilder(urlString);
		if (MapUtils.isNotEmpty(requestParams)) {
			if (sb.indexOf("?") > -1) {
				for (Map.Entry<String, Object> entry : requestParams.entrySet()) {
					sb.append("&").append(entry.getKey()).append("=").append(entry.getValue());
				}
			} else {
				sb.append("?");
				for (Map.Entry<String, Object> entry : requestParams.entrySet()) {
					sb.append(entry.getKey()).append("=").append(entry.getValue()).append("&");
				}
				sb.replace(sb.length() - 1, sb.length(), "");
			}
		}

		return sb.toString();
	}

	public static Map<String, String> splitQuery(String urlString) throws UnsupportedEncodingException, MalformedURLException {
		URL url = new URL(urlString);
		Map<String, String> query_pairs = new LinkedHashMap();
		String query = url.getQuery();
		String[] pairs = query.split("&");
		for (String pair : pairs) {
			int idx = pair.indexOf("=");
			query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
		}
		return query_pairs;
	}
}
