package io.tapdata.connector.mysql.constant;

/**
 * @author samuel
 * @Description
 * @create 2022-04-26 14:17
 **/
public enum MysqlTestItem {
	HOST_PORT("${checkHostPortInvalid}"),
	CHECK_VERSION("${checkDatabaseVersion}"),
	CHECK_CDC_PRIVILEGES("${checkDatabaseCdcPrivileges}"),
	CHECK_BINLOG_MODE("${checkBinlogMode}"),
	CHECK_BINLOG_ROW_IMAGE("${checkBinlogRowImage}"),
	CHECK_CREATE_TABLE_PRIVILEGE("${checkCreateTablePrivilege}"),
	;

	private String content;

	MysqlTestItem(String content) {
		this.content = content;
	}

	public String getContent() {
		return content;
	}
}
