package com.tapdata.entity.hazelcast;

/**
 * @author samuel
 * @Description
 * @create 2022-02-21 20:09
 **/
public enum HZLoggingType {
	JDK("jdk"),
	LOG4J("log4j"),
	LOG4J2("log4j2"),
	SLF4J("slf4j"),
	NONE("none"),
	;

	private String type;

	HZLoggingType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
