package io.tapdata.connector.dameng.cdc.logminer.sqlparser.util;

/**
 * SQL读取器
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/23 上午10:20
 * </pre>
 */
public class SQLReader {
	private String data;
	private int length;
	private int position;
	private char current;

	private SQLReader(String data) {
		this.data = data;
		this.position = 0;
		this.length = data.length();
		this.current = data.charAt(position);
	}

	public int length() {
		return length;
	}

	public int position() {
		return position;
	}

	public char current() {
		return current;
	}

	public boolean current(char c) {
		return current == c;
	}

	public void currentCheck(char c, String errorMsg) {
		if (!current(c)) {
			throw ex(errorMsg);
		}
	}

	public boolean hasNext() {
		return position + 1 < length;
	}

	public char next() {
		position++;
		current = data.charAt(position);
		return current;
	}

	public boolean next(char c) {
		return c == next();
	}

	public void moveTo(int pos) {
		current = data.charAt(pos);
		position = pos;
	}

	/**
	 * skip by size
	 *
	 * @param size skip size
	 * @return end return false
	 */
	public boolean skip(int size) {
		position += size;
		if (position < length) {
			current = data.charAt(position);
			return true;
		}
		return false;
	}

	/**
	 * skip chars
	 *
	 * @param fn skip checker
	 * @return end return false
	 */
	public boolean nextAndSkip(IChecker fn) {
		while (hasNext()) {
			if (!fn.check(next())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * equal ignore case is true, move position
	 *
	 * @param str equal str
	 * @return is move to
	 */
	public boolean equalsIgnoreCaseAndMove(String str) {
		int endPosition = position + str.length();
		if (endPosition < length) {
			if (str.equalsIgnoreCase(data.substring(position, endPosition))) {
				moveTo(endPosition);
				return true;
			}
		}
		return false;
	}

  /**
   * equal, move position
   *
   * @param str equal str
   * @return is move to
   */
  public boolean equalsAndMove(String str) {
    int endPosition = position + str.length();
    if (endPosition < length) {
      if (str.equals(data.substring(position, endPosition))) {
        moveTo(endPosition);
        return true;
      }
    }
    return false;
  }

	public String loadIn(IChecker fn, String errorMsg) {
		int begin = position;
		while (fn.check(current)) {
			next();
		}
		if (begin < position) {
			try {
				return substring(begin, position);
			} finally {
				moveTo(position - 1);
			}
		}
		throw ex(errorMsg);
	}

	public String loadNotIn(IChecker fn) {
		int begin = position;
		while (!fn.check(current)) next();
		return substring(begin, position);
	}

	public String loadInQuote(int capacity, char escape) {
		char quote = current;
		StringBuilder sb = new StringBuilder(capacity);
		while (!next(quote)) {
			if (current(escape)) next();
			sb.append(current);
		}
		return sb.toString();
	}

	public String loadInQuote(int capacity) {
		char quote = current;
		StringBuilder sb = new StringBuilder(capacity);
		while (true) {
			if (next(quote)) {
				if (hasNext()) {
					if (next(quote)) {
						sb.append(current);
						continue;
					} else {
						moveTo(position - 1);
					}
				}
				return sb.toString();
			}
			sb.append(current);
		}
	}

	public String loadInQuoteMulti(int capacity, char endQuote) {
		int layer = 0;
		char quote = current;
		StringBuilder sb = new StringBuilder(capacity);
		sb.append(current);
		while (true) {
			if (next(endQuote)) {
				if (layer <= 0) {
					sb.append(current);
					return sb.toString();
				}
				layer--;
			} else if (current(quote)) {
				layer++;
			}
			sb.append(current);
		}
	}

	public String substring(int begin, int end) {
		return data.substring(begin, end);
	}

	public RuntimeException ex(String msg) {
		return new RuntimeException(msg + ", position " + position + ": " + data);
	}

	public static SQLReader build(String data) {
		return new SQLReader(data);
	}
}
