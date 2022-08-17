package io.tapdata.connector.constant;


public enum RedisTestItem {
	HOST_PORT("Check host port is invalid"),
	CHECK_AUTH("Checks if the password and database are available"),

	;

	private String content;

	RedisTestItem(String content) {
		this.content = content;
	}

	public String getContent() {
		return content;
	}
}
