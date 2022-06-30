package com.tapdata.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RestURLInfo implements Serializable {

	public final static String URL_TYPE_INITIAL_SYNC = "INITIAL_SYNC";
	public final static String URL_TYPE_INCREMENTAL_SYNC = "INCREMENTAL_SYNC";
	public final static String URL_TYPE_GET_TOKEN = "GET_TOKEN";

	private String method;

	private String url_type;

	private String url;

	private Map<String, Object> headers;

	private Map<String, Object> request_parameters;

	private String offset_field;

	private String initial_offset;

	public RestURLInfo() {
	}

	public RestURLInfo(RestURLInfo urlInfo) {
		this.method = urlInfo.getMethod();
		this.url_type = urlInfo.getUrl_type();
		this.url = urlInfo.getUrl();
		this.headers = new HashMap<>(urlInfo.getHeaders());
		this.request_parameters = new HashMap<>(urlInfo.getRequest_parameters());
		this.offset_field = urlInfo.getOffset_field();
		this.initial_offset = urlInfo.getInitial_offset();
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getUrl_type() {
		return url_type;
	}

	public void setUrl_type(String url_type) {
		this.url_type = url_type;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Map<String, Object> getHeaders() {
		return headers;
	}

	public void setHeaders(Map<String, Object> headers) {
		this.headers = headers;
	}

	public Map<String, Object> getRequest_parameters() {
		return request_parameters;
	}

	public void setRequest_parameters(Map<String, Object> request_parameters) {
		this.request_parameters = request_parameters;
	}

	public String getOffset_field() {
		return offset_field;
	}

	public void setOffset_field(String offset_field) {
		this.offset_field = offset_field;
	}

	public String getInitial_offset() {
		return initial_offset;
	}

	public void setInitial_offset(String initial_offset) {
		this.initial_offset = initial_offset;
	}
}
