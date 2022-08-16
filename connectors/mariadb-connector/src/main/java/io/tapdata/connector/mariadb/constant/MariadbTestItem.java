package io.tapdata.connector.mariadb.constant;


public enum MariadbTestItem {
	HOST_PORT("Check host port is invalid"),
	CHECK_VERSION("Check database version"),
	CHECK_CDC_PRIVILEGES("Check database cdc privileges"),
	CHECK_BINLOG_MODE("Check binlog mode"),
	CHECK_BINLOG_ROW_IMAGE("Check binlog row image"),
	CHECK_CREATE_TABLE_PRIVILEGE("Check create table privilege"),
	;

	private String content;

	MariadbTestItem(String content) {
		this.content = content;
	}

	public String getContent() {
		return content;
	}
}
