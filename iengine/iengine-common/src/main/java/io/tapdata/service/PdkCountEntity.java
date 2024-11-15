package io.tapdata.service;

import com.tapdata.constant.Log4jUtil;
import org.apache.http.HttpStatus;

/**
 * @author samuel
 * @Description
 * @create 2024-11-13 19:19
 **/
public class PdkCountEntity {
	private int code;
	private String errorMsg;
	private String errorStack;
	private long rows;

	public PdkCountEntity() {
	}

	public PdkCountEntity success(long rows) {
		this.code = HttpStatus.SC_OK;
		this.rows = rows;
		return this;
	}

	public PdkCountEntity failed(Exception exception) {
		this.code = HttpStatus.SC_INTERNAL_SERVER_ERROR;
		if (null != exception) {
			this.errorMsg = exception.getMessage();
			this.errorStack = Log4jUtil.getStackString(exception);
		}
		return this;
	}

	public long getRows() {
		return rows;
	}

	public int getCode() {
		return code;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public String getErrorStack() {
		return errorStack;
	}
}
