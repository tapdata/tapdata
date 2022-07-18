package io.tapdata.cdc.ddl.sql;

import io.tapdata.cdc.ddl.utils.StringReader;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL 解析器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/10 下午9:00 Create
 */
public class SqlParser extends SqlBasic {
	public SqlParser() {
		super('\\', '`', '`', '.', '\'');
	}

	public SqlParser(char escape, char nameBegin, char nameEnd, char nameSplit, char valueQuota) {
		super(escape, nameBegin, nameEnd, nameSplit, valueQuota);
	}

	public String namespaceWrap(List<String> namespace) {
		List<String> names = new ArrayList<>();
		for (String n : namespace) {
			names.add(nameWrap(n));
		}
		return String.join(String.valueOf(nameSplit), names);
	}

	public String nameWrap(String val) {
		if (null == val) return null;

		char c;
		StringBuilder buf = new StringBuilder();
		buf.append(nameBegin);
		for (int i = 0, len = val.length(); i < len; i++) {
			c = val.charAt(i);
			if (nameBegin == c || escape == c) {
				buf.append('\\');
			}
			buf.append(c);
		}
		buf.append(nameEnd);
		return buf.toString();
	}

	public String nameUnwrap(String val) {
		if (null == val) return null;

		char c;
		StringBuilder buf = new StringBuilder();
		for (int i = 1, len = val.length(); i < len; i++) {
			c = val.charAt(i);
			if (escape == c) {
				i++;
				if (i >= len) break; // end by escape

				c = val.charAt(i);
			} else if (nameEnd == c) {
				i++;
				if (i >= len) break; // end by end quota
			}
			buf.append(c);
		}
		return buf.toString();
	}

	public String valueWrap(String val) {
		if (null == val) return null;

		char c;
		StringBuilder buf = new StringBuilder();
		buf.append(valueQuota);
		for (int i = 0, len = val.length(); i < len; i++) {
			c = val.charAt(i);
			if (valueQuota == c) {
				buf.append('\'');
			}
			buf.append(c);
		}
		buf.append(valueQuota);
		return buf.toString();
	}

	public String valueUnwrap(String val) {
		if (null == val) return null;

		char quota = val.charAt(0), c;
		StringBuilder buf = new StringBuilder();
		for (int i = 1, len = val.length(); i < len; i++) {
			c = val.charAt(i);
			if (valueQuota == c) {
				i++;
				if (i >= len) break; // end by escape

				c = val.charAt(i);
			} else if (quota == c) {
				i++;
				if (i >= len) break; // end by end quota
			}
			buf.append(c);
		}
		return buf.toString();
	}

	public List<String> loadNames(StringReader sr) {
		List<String> names = new ArrayList<>();
		names.add(loadName(sr));
		while (sr.nextAndSkip(this::spaceFn) && sr.current(nameSplit)) {
			sr.nextAndSkipFail2Ex(this::spaceFn, "Not has more after '" + nameSplit + "'");
			names.add(loadName(sr));
		}
		return names;
	}

	public String loadName(StringReader sr) {
		if (sr.current(nameBegin)) {
			String name = sr.loadInQuoteMulti(30, nameEnd);
			if (name.length() > 1) {
				return nameUnwrap(name);
			}
			return name;
		}
		return sr.loadNotIn(this::endNameFn);
	}
}
