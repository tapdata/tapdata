package com.tapdata.processor.util;

import com.tapdata.entity.RestURLInfo;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;

public class RestRequestEntity {

	private HttpEntity httpEntity;

	private HttpMethod httpMethod;

	private String url;

	private RestURLInfo tokenURLInfo;

	public RestRequestEntity(HttpEntity httpEntity, HttpMethod httpMethod, String url, RestURLInfo tokenURLInfo) {
		this.httpEntity = httpEntity;
		this.httpMethod = httpMethod;
		this.url = url;
		this.tokenURLInfo = tokenURLInfo;
	}

	public HttpEntity getHttpEntity() {
		return httpEntity;
	}

	public void setHttpEntity(HttpEntity httpEntity) {
		this.httpEntity = httpEntity;
	}

	public HttpMethod getHttpMethod() {
		return httpMethod;
	}

	public void setHttpMethod(HttpMethod httpMethod) {
		this.httpMethod = httpMethod;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public RestURLInfo getTokenURLInfo() {
		return tokenURLInfo;
	}

	public void setTokenURLInfo(RestURLInfo tokenURLInfo) {
		this.tokenURLInfo = tokenURLInfo;
	}
}
