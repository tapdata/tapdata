package io.tapdata.cdc.ddl.sql;

/**
 * SQL基础实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/12 上午2:11 Create
 */
public class SqlBasic {
	protected char escape;
	protected char nameBegin;
	protected char nameEnd;
	protected char nameSplit;
	protected char valueQuota;

	public SqlBasic(char escape, char nameBegin, char nameEnd, char nameSplit, char valueQuota) {
		this.escape = escape;
		this.nameBegin = nameBegin;
		this.nameEnd = nameEnd;
		this.nameSplit = nameSplit;
		this.valueQuota = valueQuota;
	}

	public boolean spaceFn(char c) {
		switch (c) {
			case ' ':
			case '\r':
			case '\n':
			case '\t':
				return true;
			default:
				return false;
		}
	}

	public boolean endNameFn(char c) {
		if (this.spaceFn(c)) return true;
		switch (c) {
			case '.':
			case '(':
			case ')':
				return true;
			default:
				return false;
		}
	}

}
