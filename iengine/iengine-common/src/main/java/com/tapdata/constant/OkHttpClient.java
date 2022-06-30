package com.tapdata.constant;

import com.fasterxml.jackson.core.JsonProcessingException;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-11-01 15:34
 **/
public class OkHttpClient {

	public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

	private static okhttp3.OkHttpClient getClient() {
		return new okhttp3.OkHttpClient.Builder().build();
	}

	public static Response get(String url, Map<String, String> headerMap) throws Exception {
		Request.Builder builder = new Request.Builder().url(url).get();
		setHeader(headerMap, builder);
		Request request = builder.build();
		try {
			return execute(request);
		} catch (IOException e) {
			throw new IOException("Call get method failed, url: " + url + ", headers: " + headerMap + ", cause: " + e.getMessage(), e);
		}
	}

	public static Response post(String url, String json, Map<String, String> headerMap) throws Exception {
		RequestBody requestBody = getRequestBody(json);
		Request.Builder builder = new Request.Builder().url(url).post(requestBody);
		setHeader(headerMap, builder);
		Request request = builder.build();
		try {
			return execute(request);
		} catch (IOException e) {
			throw new IOException("Call post method failed, url: " + url + ", body: " + json + ", headers: " + headerMap + ", cause: " + e.getMessage(), e);
		}
	}

	public static Response patch(String url, Object body, Map<String, String> headerMap) throws Exception {
		String json;
		try {
			json = JSONUtil.obj2Json(body);
		} catch (JsonProcessingException e) {
			throw new Exception("convert object to json failed, object: " + body + "; " + e.getMessage(), e);
		}
		return patch(url, json, headerMap);
	}

	public static Response patch(String url, String json, Map<String, String> headerMap) throws Exception {
		RequestBody requestBody = getRequestBody(json);
		Request.Builder builder = new Request.Builder().url(url).patch(requestBody);
		setHeader(headerMap, builder);
		Request request = builder.build();
		try {
			return execute(request);
		} catch (IOException e) {
			throw new IOException("Call post method failed, url: " + url + ", body: " + json + ", headers: " + headerMap + ", cause: " + e.getMessage(), e);
		}
	}

	public static Response delete(String url, Map<String, String> headerMap) throws Exception {
		Request.Builder builder = new Request.Builder().url(url).delete();
		setHeader(headerMap, builder);
		Request request = builder.build();
		try {
			return execute(request);
		} catch (IOException e) {
			throw new IOException("Call post method failed, url: " + url + ", headers: " + headerMap + ", cause: " + e.getMessage(), e);
		}
	}

	private static Response execute(Request request) throws Exception {
		try (okhttp3.Response response = getClient().newCall(request).execute()) {
			return new Response(
					response.code(),
					response.message(),
					response.body() == null ? "" : response.body().string()
			);
		}
	}

	@NotNull
	private static RequestBody getRequestBody(String json) {
		return RequestBody.create(json, JSON);
	}

	private static void setHeader(Map<String, String> headerMap, Request.Builder builder) {
		if (headerMap != null) {
			headerMap.forEach(builder::header);
		}
	}

	public static class Response {
		private int code;
		private String messsage;
		private String body;

		public Response(int code, String messsage, String body) {
			this.code = code;
			this.messsage = messsage;
			this.body = body;
		}

		public int getCode() {
			return code;
		}

		public String getMesssage() {
			return messsage;
		}

		public String getBody() {
			return body;
		}

		@Override
		public String toString() {
			return "Response{" +
					"code=" + code +
					", messsage='" + messsage + '\'' +
					", body='" + body + '\'' +
					'}';
		}
	}
}
