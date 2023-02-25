package com.tapdata.tm.sdk.util;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author lg
 * Create by lg on 9/3/19 5:46 PM
 */
public class SignUtil {

	private final static String ENCODING = "UTF-8";

	public static String percentEncode(String value) throws UnsupportedEncodingException {
		return value != null ? URLEncoder.encode(value, ENCODING)
				.replaceAll("\\+", "%20")
				//.replaceAll("\\*", "%2A")
				.replaceAll("%21", "!")
				.replaceAll("%27", "'")
				.replaceAll("%28", "(")
				.replaceAll("%29", ")")
				.replaceAll("%7E", "~") : null;
	}

	/**
	 * 对请求参数排序，并按照接口规范中所述"参数名=参数值"的模式用"&"字符拼接成字符串
	 *
	 * @param params
	 *            需要排序并参与字符拼接的参数
	 * @return 拼接后字符串
	 */
	public static String canonicalQueryString(final Map<String, String> params) {
		return params.keySet().stream()
				.filter(key -> !("sign".equalsIgnoreCase(key) || params.get(key) == null || "".equalsIgnoreCase(params.get(key) )))
				.sorted()
				.map(key -> {
					try {
						return percentEncode(key) + "=" + percentEncode(params.get(key));
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
					return null;
				})
				.filter(Objects::nonNull)
				.collect(Collectors.joining("&"))
				;
	}



}
