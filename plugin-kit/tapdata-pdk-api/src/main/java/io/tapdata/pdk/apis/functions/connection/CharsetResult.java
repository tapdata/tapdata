package io.tapdata.pdk.apis.functions.connection;

import io.tapdata.pdk.apis.charset.DatabaseCharset;

import java.util.*;

public class CharsetResult {
	public static final String CATEGORY_CHINESE_SIMPLIFIED = "cn_simplify";
	public static final String CATEGORY_CHINESE_TRADITIONAL = "cn_traditional";
	public static final String CATEGORY_ENGLISH = "en";
	public static final String CATEGORY_GENERIC = "generic";
	public static final String CATEGORY_DEFAULT = "default";
	public static CharsetResult create() {
		return new CharsetResult();
	}
	/**
	 * Category -> Charset
	 */
	private Map<String, List<DatabaseCharset>> charsetMap = new LinkedHashMap<>();
	public CharsetResult charset(String category, String charset) {
		List<DatabaseCharset> list = charsetMap.computeIfAbsent(category, s -> new ArrayList<DatabaseCharset>());
		if(!list.contains(charset)) {
			list.add(DatabaseCharset.create().charset(charset));
		}
		return this;
	}

	public CharsetResult charset(String charset) {
		return charset(CATEGORY_DEFAULT, charset);
	}

	public Map<String, List<DatabaseCharset>> getCharsetMap() {
		return charsetMap;
	}
	public CharsetResult setCharsetMap(Map<String, List<DatabaseCharset>> charsetMap) {
		this.charsetMap = charsetMap;
		return this;
	}

	@Override
	public String toString() {
		StringBuilder value = new StringBuilder();
		this.getCharsetMap().forEach((charset,listSet)->{
			value.append("=======>")
					.append(charset);
			for (DatabaseCharset databaseCharset : listSet) {
				value.append("   ")
						.append(databaseCharset.getCharset())
						.append("   ")
						.append(databaseCharset.getDescription());
			}
		});
		return value.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		CharsetResult that = (CharsetResult) o;
		return Objects.equals(this.toString(), that.toString());
	}

	@Override
	public int hashCode() {
		return Objects.hash(charsetMap);
	}
}
