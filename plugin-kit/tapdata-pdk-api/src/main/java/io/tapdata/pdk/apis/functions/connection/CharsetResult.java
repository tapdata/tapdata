package io.tapdata.pdk.apis.functions.connection;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CharsetResult {
	public static final String CATEGORY_CHINESE_SIMPLIFIED = "cn_simplify";
	public static final String CATEGORY_CHINESE_TRADITIONAL = "cn_traditional";
	public static final String CATEGORY_ENGLISH = "en";
	public static final String CATEGORY_GENERIC = "generic";
	public static final String CATEGORY_DEFAULT = "default";
	/**
	 * Category -> Charset
	 */
	private final Map<String, List<String>> charsetMap = new LinkedHashMap<>();
	public CharsetResult charset(String category, String charset) {
		List<String> list = charsetMap.computeIfAbsent(category, s -> new ArrayList<>());
		if(!list.contains(charset)) {
			list.add(charset);
		}
		return this;
	}

	public CharsetResult charset(String charset) {
		return charset(CATEGORY_DEFAULT, charset);
	}

	public Map<String, List<String>> getCharsetMap() {
		return charsetMap;
	}
}
