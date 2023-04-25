package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapCodeException;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-02-23 11:15
 **/
public class TapRecordException extends TapCodeException {

	private static final long serialVersionUID = 4981529659601020760L;

	public TapRecordException(String code) {
		super(code);
	}

	public TapRecordException(String code, String message) {
		super(code, message);
	}

	public TapRecordException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public TapRecordException(String code, Throwable cause) {
		super(code, cause);
	}

	private List<Map<String, Object>> records;

	public void setRecords(List<Map<String, Object>> records) {
		this.records = records;
	}

	public List<Map<String, Object>> getRecords() {
		return records;
	}

	public TapRecordException records(List<Map<String, Object>> records) {
		this.records = records;
		return this;
	}
}
