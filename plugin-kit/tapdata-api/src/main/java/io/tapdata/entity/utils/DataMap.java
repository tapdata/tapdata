package io.tapdata.entity.utils;


import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class DataMap extends LinkedHashMap<String, Object> {
	private String keyRegex;
	private Pattern keyPattern;

	public DataMap keyRegex(String keyRegex) {
		this.keyRegex = keyRegex;
		if (keyRegex != null)
			keyPattern = Pattern.compile(keyRegex, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
		return this;
	}

	private String prefix;

	public DataMap prefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	public static DataMap create(Map<String, Object> map) {
		DataMap dataMap = new DataMap();
		if (map != null) {
			dataMap.putAll(map);
		}
		return dataMap;
	}

	public static DataMap create() {
		return new DataMap();
	}

	public <T> DataMap kv(String key, T value) {
		if (keyPattern != null) {
			final Matcher matcher = keyPattern.matcher(key);
			if (!matcher.matches())
				return this;
		}
		if (prefix != null)
			key = prefix + "_" + key;
		super.put(key, value);
		return this;
	}

	public <T> T getValue(String key, T defaultValue) {
		T value = (T) super.get(key);
		if (value == null) {
			return defaultValue;
		}
		return value;
	}

	public String getString(String key) {
		Object o = super.get(key);
		if (null == o) return null;
		return String.valueOf(o);
	}

	public Integer getInteger(String key) {
		String string = getString(key);
		if (null == string || "".equals(string.trim())) return null;
		return Integer.valueOf(string);
	}

	public Object getObject(String key) {
		return super.get(key);
	}


}
