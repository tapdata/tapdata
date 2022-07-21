package io.tapdata.ddlp.utils;

import java.util.function.Function;

/**
 * SQL 解析器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/22 下午2:29 Create
 */
public class CharReader {

	private String data;
	private int pos;

	public CharReader(String data) {
		this.data = data;
	}

	public String data() {
		return data;
	}

	public int pos() {
		return pos;
	}

	public int len() {
		return data.length();
	}

	public char current() {
		return data.charAt(pos);
	}

	public void seek(int pos) {
		this.pos = pos;
	}

	public boolean is(char c) {
		return current() == c;
	}

	public boolean hasNext() {
		return pos + 1 < data.length();
	}

	public char next() {
		return data.charAt(++pos);
	}

	public char next(String errMsg) {
		if (hasNext()) {
			return next();
		} else {
			throw ex(errMsg);
		}
	}

	public String substring(int begin, int end) {
		return data.substring(begin, end);
	}

	public String substring(int begin) {
		return data.substring(begin);
	}

	/**
	 * 检查是否包含字符串
	 *
	 * @param ignoreCase 忽略大小写
	 * @param existsMove 存在则移动下标
	 * @param arr        字符串集合
	 * @return 存在的字符串下标，-1：则不存在
	 */
	public int checkIn2Index(boolean ignoreCase, boolean existsMove, String... arr) {
		int ret = -1;
		if (null == arr) return ret;

		int endPosition;
		String str;
		for (int i = 0, len = arr.length; i < len; i++) {
			str = arr[i];
			if (str.length() == 0) continue;
			endPosition = pos + str.length();
			if (endPosition <= len() && (ignoreCase
					? str.equalsIgnoreCase(substring(pos, endPosition))
					: str.equals(substring(pos, endPosition)))
			) {
				if (existsMove) seek(endPosition - 1);
				return i;
			}
		}
		return ret;
	}

	/**
	 * 跳过包含字符
	 *
	 * @param fn 检查函数
	 * @return 是否还有数据
	 */
	public boolean skipIn(Function<Character, Boolean> fn) {
		if (!fn.apply(current())) return hasNext();
		while (hasNext()) {
			if (!fn.apply(next())) {
				seek(pos - 1);
				return true;
			}
		}
		return false;
	}

	/**
	 * 下一个并跳过包含字符
	 *
	 * @param fn 检查函数
	 * @return 是否还有数据
	 */
	public boolean nextAndSkipIn(Function<Character, Boolean> fn) {
		while (hasNext()) {
			if (!fn.apply(next())) {
				return true;
			}
		}
		return false;
	}

	public void nextAndSkipIn(Function<Character, Boolean> fn, String errMsg) {
		if (!nextAndSkipIn(fn)) throw ex(errMsg);
	}

	/**
	 * 加载包含的字符
	 *
	 * @param fn 检查函数
	 * @return 字符串
	 */
	public String readIn(Function<Character, Boolean> fn) {
		char curr = current();
		StringBuilder sb = new StringBuilder();
		if (!fn.apply(curr)) return null;
		sb.append(curr);
		while (hasNext()) {
			curr = next();

			if (!fn.apply(curr)) {
				seek(pos - 1);
				break;
			}
			sb.append(curr);
		}

		return sb.toString();
	}

	/**
	 * 加载不包含的字符
	 *
	 * @param fn 检查函数
	 * @return 字符串
	 */
	public String readNotIn(Function<Character, Boolean> fn) {
		char curr = current();
		StringBuilder sb = new StringBuilder();
		if (fn.apply(curr)) return null;
		sb.append(curr);
		while (hasNext()) {
			curr = next();

			if (fn.apply(curr)) {
				seek(pos - 1);
				break;
			}
			sb.append(curr);
		}

		return sb.toString();
	}

	/**
	 * 加载不包含的字符
	 *
	 * @param fn     检查函数
	 * @param errMsg 错误提示
	 * @return 字符串
	 */
	public String readNotIn(Function<Character, Boolean> fn, String errMsg) {
		String val = readNotIn(fn);
		if (null == val) throw ex(errMsg);
		return val;
	}

	/**
	 * 加载包裹的字符串
	 *
	 * @return 被包裹字符串
	 */
	public String readInQuote() {
		char quote = current();
		return readInQuote(quote, quote);
	}

	/**
	 * 加载包裹的字符串
	 *
	 * @param begin 前引号
	 * @param end   后引号
	 * @return 被包裹字符串
	 */
	public String readInQuote(char begin, char end) {
		char curr = current();
		if (curr != begin) return null;

		StringBuilder sb = new StringBuilder();
		while (hasNext()) {
			curr = next();

			if (curr == end) return sb.toString();
			sb.append(curr);
		}

		throw ex("Not found end quote: '" + end + "'");
	}

	/**
	 * 加载包裹的字符串
	 *
	 * @param begin  前引号
	 * @param end    后引号
	 * @param escape 转义符
	 * @return 被包裹字符串
	 */
	public String readInQuote(char begin, char end, char escape, char... escapeChars) {
		char curr = current();
		if (curr != begin) return null;

		StringBuilder sb = new StringBuilder();
		while (hasNext()) {
			curr = next();

			if (curr == escape && hasNext()) {
				boolean isSet = false;
				char netTmp = next();
				for (char c : escapeChars) {
					if (netTmp == c) {
						sb.append(curr).append(netTmp);
						isSet = true;
					}
				}
				if (isSet) continue;
				pos--;
			}

			if (curr == end) return sb.toString();
			sb.append(curr);
		}

		throw ex("Not found end quote: '" + end + "'");
	}

	public RuntimeException ex(String msg) {
		return new RuntimeException(msg + ", position " + pos() + ": " + data());
	}

	@Override
	public String toString() {
		return "CharReader{" +
				"data='" + data + '\'' +
				", pos=" + pos +
				'}';
	}
}
