package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;

public class JavaScriptFunctions {

	public static final String JAVA_TYPE_TEMPLATE = "var %s = Java.type(\"%s\");";

	private String function_name;

	private String parameters;

	private String function_body;

	private String last_update;

	private String user_id;

	/**
	 * jar or ...
	 */
	private String type;

	private String className;

	/**
	 * jar文件存在mongodb中的objectId
	 */
	private String fileId;

	public JavaScriptFunctions() {
	}

	public String getJSFunction() {
		if (isJar()) {
			if (StringUtils.isNotBlank(function_name) && StringUtils.isNotBlank(className)) {
				return String.format(JAVA_TYPE_TEMPLATE, function_name, className);
			}
		} else {
			if (StringUtils.isNotBlank(function_name) && StringUtils.isNotBlank(function_body)) {
				StringBuilder sb = new StringBuilder("function ").append(function_name).append("(");
				if (StringUtils.isNotBlank(parameters)) {
					sb.append(parameters);
				}

				sb.append(")").append(function_body);

				return sb.toString();
			}
		}

		return null;
	}

	public boolean isJar() {
		return StringUtils.equalsIgnoreCase(type, "jar");
	}

	public boolean isSystem() {
		return StringUtils.equalsIgnoreCase(type, "system");
	}

	public String getFunction_name() {
		return function_name;
	}

	public void setFunction_name(String function_name) {
		this.function_name = function_name;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public String getFunction_body() {
		return function_body;
	}

	public void setFunction_body(String function_body) {
		this.function_body = function_body;
	}

	public String getLast_update() {
		return last_update;
	}

	public void setLast_update(String last_update) {
		this.last_update = last_update;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getFileId() {
		return fileId;
	}

	public void setFileId(String fileId) {
		this.fileId = fileId;
	}
}
