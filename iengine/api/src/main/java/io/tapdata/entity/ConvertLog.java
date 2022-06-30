package io.tapdata.entity;

public enum ConvertLog {
	/**
	 * error
	 */

	// jdbc
	ERR_JDBC_0001("ERR_JDBC_0001", "convert type error - %s, table name: %s, field name: %s, value: %s"),
	ERR_JDBC_0002("ERR_JDBC_0002", "convert date type error - %s"),
	ERR_JDBC_0003("ERR_JDBC_0003", "convert clob type error - %s"),
	ERR_JDBC_0004("ERR_JDBC_0004", "convert blob type error - %s"),
	ERR_JDBC_0005("ERR_JDBC_0005", "convert %s value type error, type %s to %s - %s"),

	// mongodb
	ERR_MONGO_0001("ERR_MONGO_0001", "convert %s to %s error - %s"),
	ERR_MONGO_0002("ERR_MONGO_0002", "field name convert error: %s"),

	// oracle
	ERR_ORA_0001("ERR_ORA_0001", "convert %s to %s error - %s"),

	// common target value
	ERR_COMMON_TARGET_0001("ERR_COMMON_TARGET_0001", "convert value %s error, type %s to %s - %s"),

	// mysql java type convert
	ERR_MYSQL_JAVATYPE_0001("convert java type %s to %s error, message: %s"),

	// oracle java type convert
	ERR_ORACLE_JAVATYPE_0001("convert java type %s to %s error, message: %s"),
	;

	private String errCode;
	private String message;

	ConvertLog(String errCode, String message) {
		this.errCode = errCode;
		this.message = message;
	}

	ConvertLog(String message) {
		this.message = message;
	}

	public String getErrCode() {
		return errCode;
	}

	public String getMessage() {
		return message;
	}
}
