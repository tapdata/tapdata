package com.tapdata.constant;

/**
 * 引号包装工具
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/31 下午4:40
 * </pre>
 */
public interface QuotaWrap {
	char ESCAPE = '\\';
	QuotaWrap EMPTY = new QuotaWrap() {
		@Override
		public boolean wrap(char c, StringBuilder buf) {
			return false;
		}

		@Override
		public boolean unwrap(char c, StringBuilder buf) {
			return false;
		}
	};
	QuotaWrap UN_QUOTA = new QuotaWrap() {
		@Override
		public boolean wrap(char c, StringBuilder buf) {
			switch (c) {
				case '\b':
					buf.append("\\b");
					return true;
				case '\f':
					buf.append("\\f");
					return true;
				case '\n':
					buf.append("\\n");
					return true;
				case '\r':
					buf.append("\\r");
					return true;
				case '\t':
					buf.append("\\t");
					return true;
				default:
					return false;
			}
		}

		@Override
		public boolean unwrap(char c, StringBuilder buf) {
			switch (c) {
				case 'b':
					buf.append('\b');
					return true;
				case 'f':
					buf.append('\f');
					return true;
				case 'n':
					buf.append('\n');
					return true;
				case 'r':
					buf.append('\r');
					return true;
				case 't':
					buf.append('\t');
					return true;
				default:
					return false;
			}
		}
	};

	boolean wrap(char c, StringBuilder buf);

	boolean unwrap(char c, StringBuilder buf);

	static String wrap(char quota, String val) {
		return wrap(quota, ESCAPE, val, EMPTY);
	}

	static String wrap(char quota, char escape, String val) {
		return wrap(quota, escape, val, EMPTY);
	}

	static String unwrap(String val) {
		return unwrap(ESCAPE, val, EMPTY);
	}

	static String unwrap(char escape, String val) {
		return unwrap(escape, val, EMPTY);
	}

	static String wrapSqlDefaultValue(String val) {
		return wrap('\'', '\'', val, EMPTY);
	}

	static String unwrapSqlDefaultValue(String val) {
		return unwrap('\'', val, EMPTY);
	}

	static String wrap(char quota, char escape, String val, QuotaWrap parser) {
		if (null == val) return null;

		char c;
		StringBuilder buf = new StringBuilder();
		buf.append(quota);
		for (int i = 0, len = val.length(); i < len; i++) {
			c = val.charAt(i);
			if (!parser.wrap(c, buf)) {
				if (quota == c) {
					buf.append(escape);
				} else if (escape == c) {
					buf.append(escape);
				}
				buf.append(c);
			}
		}
		buf.append(quota);
		return buf.toString();
	}

	static String unwrap(char escape, String val, QuotaWrap parser) {
		if (null == val) return null;

		char quota = val.charAt(0), c;
		StringBuilder buf = new StringBuilder();
		for (int i = 1, len = val.length(); i < len; i++) {
			c = val.charAt(i);
			if (escape == c) {
				i++;
				if (i >= len) break; // end by escape

				c = val.charAt(i);
				if (parser.unwrap(c, buf)) {
					continue;
				}
			} else if (quota == c) {
				continue;
			}
			buf.append(c);
		}
		return buf.toString();
	}

	static void main(String[] args) {
		char escape = '\'';
		String before, after = "--\'--\"--\n--\t--\r--\b--";
		System.out.println((before = after) + " >>> " + (after = wrap('\'', escape, before)) + " >>> " + unwrap(escape, after).equals(before));
		System.out.println((before = after) + " >>> " + (after = wrap('"', escape, before)) + " >>> " + unwrap(escape, after).equals(before));
		System.out.println((before = after) + " >>> " + (after = wrap('\'', escape, before)) + " >>> " + unwrap(escape, after).equals(before));
	}
}
