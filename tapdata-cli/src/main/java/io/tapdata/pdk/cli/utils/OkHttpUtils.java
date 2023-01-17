package io.tapdata.pdk.cli.utils;

import okhttp3.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * Description: okhttp调用外部接口工具类
 *
 * @author: Zed
 * Date: 2018-10-12
 * Time: 13:55
 */
public class OkHttpUtils {

	private static OkHttpClient okHttpClient;

	static {
		okHttpClient = new OkHttpClient.Builder().connectTimeout(10, TimeUnit.MINUTES)
				.readTimeout(10, TimeUnit.MINUTES)
				.writeTimeout(10, TimeUnit.MINUTES).build();
	}

	/**
	 * 调用okhttp的newCall方法
	 *
	 * @param request 请求
	 * @return 调用返回的json
	 */
	private static String execNewCall(Request request) {
		Response response = null;
		try {
			response = okHttpClient.newCall(request).execute();
			if (response.isSuccessful() && response.body() != null) {
				return response.body().string();
			}
		} catch (Exception e) {
		} finally {
			if (response != null) {
				response.close();
			}
		}
		return null;
	}

	/**
	 * Post请求发送JSON数据....{"name":"zhangsan","pwd":"123456"}
	 *
	 * @param url        请求的url
	 * @param jsonParams JSON字符串的参数
	 */
	public static String postJsonParams(String url, String jsonParams) {
		RequestBody requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), jsonParams);
		Request request = new Request.Builder()
				.url(url)
				.post(requestBody)
				.build();
		return execNewCall(request);
	}
}
