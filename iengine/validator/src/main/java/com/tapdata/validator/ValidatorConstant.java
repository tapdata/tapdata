package com.tapdata.validator;

import com.tapdata.entity.DatabaseTypeEnum;

import java.util.HashMap;
import java.util.Map;

public class ValidatorConstant {

	/**
	 * connection status
	 */
	public static final String CONNECTION_STATUS_TESTING = "testing";
	public static final String CONNECTION_STATUS_INVALID = "invalid";
	public static final String CONNECTION_STATUS_READY = "ready";


	/**
	 * validate detail result
	 */
	public static final String VALIDATE_DETAIL_RESULT_PASSED = "passed";
	public static final String VALIDATE_DETAIL_RESULT_FAIL = "failed";
	public static final String VALIDATE_DETAIL_RESULT_WAITING = "waiting";

	/**
	 * connection validate type code - oracle
	 */
	public static final String VALIDATE_CODE_ORACLE_HOST_IP = "validate-1000";
	public static final String VALIDATE_CODE_ORACLE_USERNAME_PASSWORD = "validate-1100";
	public static final String VALIDATE_CODE_ORACLE_DATABASE_VERSION = "validate-1200";
	public static final String VALIDATE_CODE_ORACLE_OWNER_SCHEMA = "validate-1300";
	public static final String VALIDATE_CODE_ORACLE_CDC_PRIVILEGE = "validate-1400";
	public static final String VALIDATE_CODE_ORACLE_ARCHIVE_LOG_MODE = "validate-1500";
	public static final String VALIDATE_CODE_ORACLE_SUPPLEMENTAL_LOG_MODE = "validate-1600";
	public static final String VALIDATE_CODE_DDL_PRIVILEGES = "validate-1700";

	/**
	 * connection validate type code - mysql
	 */
	public static final String VALIDATE_CODE_MYSQL_HOST_IP = "validate-2000";
	public static final String VALIDATE_CODE_MYSQL_USERNAME_PASSWORD = "validate-2100";
	public static final String VALIDATE_CODE_MYSQL_DATABASE_ACCESSIBLE = "validate-2200";
	public static final String VALIDATE_CODE_MYSQL_DATABASE_VERSION = "validate-2300";
	public static final String VALIDATE_CODE_MYSQL_DATABASE_SCHEMA = "validate-2400";
	public static final String VALIDATE_CODE_MYSQL_BIN_LOG_MODE = "validate-2500";
	public static final String VALIDATE_CODE_MYSQL_BIN_LOG_ROW_IMAGE = "validate-2501";
	public static final String VALIDATE_CODE_MYSQL_CDC_PRIVILEGES = "validate-2600";
	public static final String VALIDATE_CODE_MYSQL_CREATE_TABLE_PRIVILEGES = "validate-2700";

	/**
	 * connection validate type code - mysql pxc
	 */
	public static final String VALIDATE_CODE_MYSQL_PXC_GTID_STATUS = "validate-2700";
	public static final String VALIDATE_CODE_MYSQL_PXC_BIN_LOG_SYNC = "validate-2800";

	/**
	 * connection validate type code - mongodb
	 */
	public static final String VALIDATE_CODE_MONGODB_HOST_IP = "validate-3000";
	public static final String VALIDATE_CODE_MONGODB_USERNAME_PASSWORD = "validate-3100";
	public static final String VALIDATE_CODE_MONGODB_DATABASE_SCHEMA = "validate-3200";
	//    public static final String VALIDATE_CODE_MONGODB_NODES_HOSTNAME = "validate-3300";
	public static final String VALIDATE_CODE_MONGODB_PRIVILEGES = "validate-3400";
	public static final String VALIDATE_CODE_MONGODB_CDC = "validate-3500";

	/**
	 * connection validate type code - mssql
	 */
	public static final String VALIDATE_CODE_MSSQL_HOST_IP = "validate-4000";
	public static final String VALIDATE_CODE_MSSQL_USERNAME_PASSWORD = "validate-4100";
	public static final String VALIDATE_CODE_MSSQL_DATABASE_VERSION = "validate-4200";
	public static final String VALIDATE_CODE_MSSQL_CDC_PRIVILEGES = "validate-4400";
	public static final String VALIDATE_CODE_MSSQL_OWNER_SCHEMA = "validate-4300";

	/**
	 * connection validate type code - sybase ase
	 */
	public static final String VALIDATE_CODE_SYBASE_ASE_HOST_IP = "validate-5000";
	public static final String VALIDATE_CODE_SYBASE_ASE_USERNAME_PASSWORD = "validate-5100";
	public static final String VALIDATE_CODE_SYBASE_ASE_OWNER_SCHEMA = "validate-5200";
	public static final String VALIDATE_CODE_SYBASE_ASE_USER_CDC_AUTH = "validate-5300";
	public static final String VALIDATE_CODE_SYBASE_ASE_USER_AUTH = "validate-5400";

	/**
	 * connection validate type code - excel
	 */
	public static final String VALIDATE_CODE_EXCEL_READABLE_PRIVILEGE = "validate-6000";
	public static final String VALIDATE_CODE_EXCEL_SCHEMA = "validate-6100";

	/**
	 * connection validate type code - gridfs
	 */
	public static final String VALIDATE_CODE_GRIDFS_DIR_EXISTS = "validate-7000";
	public static final String VALIDATE_CODE_GRIDFS_DIR_EMPTY = "validate-7100";

	/**
	 * connection validate type code - redis
	 */
	public static final String VALIDATE_CODE_REDIS_HOST_IP = "validate-8000";
	public static final String VALIDATE_CODE_REDIS_DB_INDEX = "validate-8100";

	/**
	 * connection error code - oracle
	 */
	public static final String ERROR_CODE_ORACLE_HOST_IP = "error-1000";
	public static final String ERROR_CODE_ORACLE_USERNAME_PASSWORD = "error-1100";
	public static final String ERROR_CODE_ORACLE_DATABASE_VERSION = "error-1200";
	public static final String ERROR_CODE_ORACLE_OWNER_SCHEMA = "error-1300";
	public static final String ERROR_CODE_ORACLE_CDC_PRIVILEGE = "error-1400";
	public static final String ERROR_CODE_ORACLE_ARCHIVE_LOG_MODE = "error-1500";
	public static final String ERROR_CODE_ORACLE_SUPPLEMENTAL_LOG_MODE = "error-1600";
	public static final String ERROR_CODE_ORACLE_DDL_PRIVILEGES = "error-1700";

	/**
	 * connection error code - mysql
	 */
	public static final String ERROR_CODE_MYSQL_HOST_IP = "error-2000";
	public static final String ERROR_CODE_MYSQL_USERNAME_PASSWORD = "error-2100";
	public static final String ERROR_CODE_MYSQL_DATABASE_ACCESSIBLE = "error-2200";
	public static final String ERROR_CODE_MYSQL_DATABASE_VERSION = "error-2300";
	public static final String ERROR_CODE_MYSQL_DATABASE_SCHEMA = "error-2400";
	public static final String ERROR_CODE_MYSQL_BIN_LOG_MODE = "error-2500";
	public static final String ERROR_CODE_MYSQL_CDC_PRIVILEGES = "error-2600";

	/**
	 * connection error code - mysql pxc
	 */
	public static final String ERROR_CODE_MYSQL_PXC_GTID_STATUS = "error-2700";
	public static final String ERROR_CODE_MYSQL_PXC_BIN_LOG_SYNC = "error-2800";

	/**
	 * connection error code - mongodb
	 */
	public static final String ERROR_CODE_MONGODB_HOST_IP = "error-3000";
	public static final String ERROR_CODE_MONGODB_USERNAME_PASSWORD = "error-3100";
	public static final String ERROR_CODE_MONGODB_DATABASE_SCHEMA = "error-3200";
	//    public static final String ERROR_CODE_MONGODB_NODES_HOSTNAME = "error-3300";
	public static final String ERROR_CODE_MONGODB_PRIVILEGES = "error-3400";
	public static final String ERROR_CODE_MONGODB_CDC = "error-3500";

	/**
	 * connection error code - mssql
	 */
	public static final String ERROR_CODE_MSSQL_HOST_IP = "error-4000";
	public static final String ERROR_CODE_MSSQL_USERNAME_PASSWORD = "error-4100";
	public static final String ERROR_CODE_MSSQL_DATABASE_VERSION = "error-4200";
	public static final String ERROR_CODE_MSSQL_OWNER_SCHEMA = "error-4300";
	public static final String ERROR_CODE_MSSQL_CDC_PRIVILEGES = "error-4400";

	/**
	 * connection error code - sybase
	 */
	public static final String ERROR_CODE_SYBASE_ASE_HOST_IP = "error-5000";
	public static final String ERROR_CODE_SYBASE_ASE_USERNAME_PASSWORD = "error-5100";
	public static final String ERROR_CODE_SYBASE_ASE_OWNER_SCHEMA = "error-5200";
	public static final String ERROR_CODE_SYBASE_ASE_USER_CDC_AUTH = "error-5300";
	public static final String ERROR_CODE_SYBASE_ASE_USER_AUTH = "error-5400";

	/**
	 * connection error code - excel
	 */
	public static final String ERROR_CODE_EXCEL_READABLE_PRIVILEGE = "error-6000";
	public static final String ERROR_CODE_EXCEL_SCHEMA = "error-6100";

	/**
	 * connection error code - gridfs
	 */
	public static final String ERROR_CODE_GRIDFS_DIR_EXIST = "error-7000";
	public static final String ERROR_CODE_GRIDFS_DIR_EMPTY = "error-7100";

	/**
	 * connection error code - redis
	 */
	public static final String ERROR_CODE_REDIS_HOST_IP = "error-8000";
	public static final String ERROR_CODE_REDIS_DB_INDEX = "error-8100";

	public enum ValidateClassEnum {
		ORACLE_VALIDATE_CLASS(DatabaseTypeEnum.ORACLE.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		MYSQL_VALIDATE_CLASS(DatabaseTypeEnum.MYSQL.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		MYSQL_PXC_VALIDATE_CLASS(DatabaseTypeEnum.MYSQL_PXC.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		KUNDB_VALIDATE_CLASS(DatabaseTypeEnum.KUNDB.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		ADB_MYSQL_VALIDATE_CLASS(DatabaseTypeEnum.ADB_MYSQL.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		ALIYUN_MYSQL_VALIDATE_CLASS(DatabaseTypeEnum.ALIYUN_MYSQL.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		MSSQL_VALIDATE_CLASS(DatabaseTypeEnum.MSSQL.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		ALIYUN_MSSQL_VALIDATE_CLASS(DatabaseTypeEnum.ALIYUN_MSSQL.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		SYBASE_ASE_VALIDATE_CLASS(DatabaseTypeEnum.SYBASEASE.getType(), "com.tapdata.validator.JdbcValidateDataSource"),
		MONGODB_VALIDATE_CLASS(DatabaseTypeEnum.MONGODB.getType(), "com.tapdata.validator.MongodbValidateDataSource"),
		;

		private String databaseType;
		private String validateClassName;

		ValidateClassEnum(String databaseType, String validateClassName) {
			this.databaseType = databaseType;
			this.validateClassName = validateClassName;
		}

		public String getDatabaseType() {
			return databaseType;
		}

		public String getValidateClassName() {
			return validateClassName;
		}

		private static final Map<String, ValidateClassEnum> map = new HashMap<>();

		static {
			for (ValidateClassEnum validateClassEnum : ValidateClassEnum.values()) {
				map.put(validateClassEnum.getDatabaseType(), validateClassEnum);
			}
		}

		public static ValidateClassEnum fromString(String databaseType) {
			return map.get(databaseType);
		}
	}
}
