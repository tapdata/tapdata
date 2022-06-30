package io.tapdata.entity;

import java.util.List;

public class LoadSchemaResult<T> {

	private List<T> schema;

	private String errMessage;

	private Throwable throwable;

	private boolean required = false;

	public List<T> getSchema() {
		return schema;
	}

	public void setSchema(List<T> schema) {
		this.schema = schema;
	}

	public String getErrMessage() {
		return errMessage;
	}

	public void setErrMessage(String errMessage) {
		this.errMessage = errMessage;
	}

	public void setErrMessage(String errMessage, Throwable throwable) {
		this.errMessage = errMessage;
		this.throwable = throwable;
	}

	public Throwable getThrowable() {
		return throwable;
	}

	public void setThrowable(Throwable throwable) {
		this.throwable = throwable;
	}

	public boolean getRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}
}
