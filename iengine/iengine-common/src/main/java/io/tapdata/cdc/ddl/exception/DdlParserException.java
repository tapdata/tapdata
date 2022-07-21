package io.tapdata.cdc.ddl.exception;

/**
 * DDL 解析异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午7:38 Create
 */
public class DdlParserException extends DdlException {
	private String ddl;

	public DdlParserException(String ddl) {
		this.ddl = ddl;
	}

	public DdlParserException(String message, String ddl) {
		super(message);
		this.ddl = ddl;
	}

	public DdlParserException(String message, Throwable cause, String ddl) {
		super(message, cause);
		this.ddl = ddl;
	}

	public DdlParserException(Throwable cause, String ddl) {
		super(cause);
		this.ddl = ddl;
	}

	public String getDdl() {
		return ddl;
	}
}
