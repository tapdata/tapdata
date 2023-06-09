package com.tapdata.constant;

import org.apache.commons.lang3.StringUtils;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StringUtil {

	private final static Pattern COMMENT_PATTERN = Pattern.compile("[^:]//.*|/\\\\*((?!=*/)(?s:.))+\\\\*/");

	public static String removeSuffix(String str, String suffix) {
		String ret = "";

		if (StringUtils.isNotBlank(str)
				&& StringUtils.isNotBlank(suffix)
				&& StringUtils.endsWithIgnoreCase(str, suffix)) {
			ret = StringUtils.removeEndIgnoreCase(str, suffix);
		}

		return ret;
	}

	public static String subStringBetweenTwoString(String sql, String start, String end) {
		String sub = "";

		if (StringUtils.isBlank(sql)) {
			return "";
		}

		int startIndex = StringUtils.indexOfIgnoreCase(sql, start);
		if (startIndex <= 0) {
			return "";
		}
		int endIndex = StringUtils.indexOfIgnoreCase(sql, end);
		if (endIndex <= 0) {
			return "";
		}

		startIndex = startIndex + start.length();
		if (startIndex >= endIndex) {
			throw new RuntimeException(String.format("Invalid sql: %s, start: %s, end: %s", sql, start, end));
		} else {
			sub = sql.substring(startIndex + start.length(), endIndex);
		}

		return sub;
	}

	public static Pattern getCommentPattern() {
		return COMMENT_PATTERN;
	}

	public static String removeComments(String code) throws Exception {
		StringBuilder newCode = new StringBuilder();
		try (StringReader sr = new StringReader(code)) {
			boolean inBlockComment = false;
			boolean inLineComment = false;
			boolean out = true;

			int prev = sr.read();
			int cur;
			for (cur = sr.read(); cur != -1; cur = sr.read()) {
				if (inBlockComment) {
					if (prev == '*' && cur == '/') {
						inBlockComment = false;
						out = false;
					}
				} else if (inLineComment) {
					if (cur == '\r') { // start untested block
						sr.mark(1);
						int next = sr.read();
						if (next != '\n') {
							sr.reset();
						}
						inLineComment = false;
						out = false; // end untested block
					} else if (cur == '\n') {
						inLineComment = false;
						out = false;
					}
				} else {
					if (prev == '/' && cur == '*') {
						sr.mark(1); // start untested block
						int next = sr.read();
						if (next != '*') {
							inBlockComment = true; // tested line (without rest of block)
						}
						sr.reset(); // end untested block
					} else if (prev == '/' && cur == '/') {
						inLineComment = true;
					} else if (out) {
						newCode.append((char) prev);
					} else {
						out = true;
					}
				}
				prev = cur;
			}
			if (prev != -1 && out && !inLineComment) {
				newCode.append((char) prev);
			}
		} catch (Exception e) {
			throw e;
		}

		return newCode.toString().trim();
	}

	public static List<String> splitKey2List(String field, String splitStr) {
		String[] split = field.split(splitStr);
		if (split == null || split.length <= 0) {
			return null;
		}

		List<String> keys = Arrays.stream(split).filter(key -> StringUtils.isNotBlank(key)).collect(Collectors.toList());

		return keys;
	}

	/**
	 * Intercept the string, if the length of the string is less than or equal to the specified length, return the original string, otherwise return the string of the specified length + specified suffix
	 * @param originStr
	 * @param keepLength
	 * @return
	 */
	public static String subLongString(String originStr, int keepLength, String suffix) {
		if (StringUtils.isBlank(originStr)) {
			return "";
		}

		if (originStr.length() <= keepLength) {
			return originStr;
		}

		return originStr.substring(0, keepLength) + suffix;
	}
}
