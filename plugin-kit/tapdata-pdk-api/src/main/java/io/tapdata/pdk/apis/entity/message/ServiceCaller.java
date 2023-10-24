package io.tapdata.pdk.apis.entity.message;

import java.util.Date;
import java.util.Map;

public class ServiceCaller extends EngineMessage {
	public static final String KEY_PREFIX = "ServiceCaller_";
	public static ServiceCaller create(String id) {
		ServiceCaller serviceCaller = new ServiceCaller();
		serviceCaller.setId(id);
		return serviceCaller;
	}
	private String className;
	public ServiceCaller className(String className) {
		this.className = className;
		return this;
	}
	private String method;
	public ServiceCaller method(String method) {
		this.method = method;
		return this;
	}
	private Object[] args;
	public ServiceCaller args(Object[] args) {
		this.args = args;
		return this;
	}

	private Integer argCount;
	private String returnClass;
	public ServiceCaller returnClass(String returnClass) {
		this.returnClass = returnClass;
		return this;
	}

	@Override
	public String toString() {
		return "ServiceCaller id " + id + " className " + className + " method " + method + " args " + args + " returnClass " + returnClass + ";";
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

	public String getReturnClass() {
		return returnClass;
	}

	public void setReturnClass(String returnClass) {
		this.returnClass = returnClass;
	}

	public Integer getArgCount() {
		return argCount;
	}

	public void setArgCount(Integer argCount) {
		this.argCount = argCount;
	}

	@Override
	public String key() {
		return KEY_PREFIX + className + "#" + method;
	}

	public String matchingKey(String className, String method) {
		StringBuilder builder = new StringBuilder(KEY_PREFIX);
		if(className != null)
			builder.append(className);
		if(method != null)
			builder.append(method);
		return builder.toString();
	}
}
