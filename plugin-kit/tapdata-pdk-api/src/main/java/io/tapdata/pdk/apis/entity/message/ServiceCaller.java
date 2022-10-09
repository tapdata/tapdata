package io.tapdata.pdk.apis.entity.message;

import java.util.Date;
import java.util.Map;

public class ServiceCaller extends EngineMessage {
	private String className;
	private String method;
	private Object[] args;

	@Override
	public String toString() {
		return "ServiceCaller id " + id + " className " + className + " method " + method + " args " + args + ";";
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public Object[] getArgs() {
		return args;
	}

	public void setArgs(Object[] args) {
		this.args = args;
	}

}
