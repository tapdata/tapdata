package com.tapdata.tm.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/10/10 4:58 下午
 * @description
 */
public class ThreadLocalUtils {

	public final static String USER_LOCALE = "USER_LOCAL";
	public final static String REQUEST_ID = "REQUEST_ID";

	private final static ThreadLocal<Map<String, Object>> threadLocal = new ThreadLocal<>();

	public static void set(String key, Object value) {
		if (threadLocal.get() == null) {
			threadLocal.set(new HashMap<>());
		}
		threadLocal.get().put(key, value);
	}

	public static <T> T get(String key) {
		Map<String, Object> map = threadLocal.get();
		if (map != null) {
			return (T) map.get(key);
		}
		return null;
	}

}
