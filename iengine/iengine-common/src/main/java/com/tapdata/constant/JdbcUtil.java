package com.tapdata.constant;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.RelateDatabaseField;
import com.tapdata.entity.SchemaBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class JdbcUtil {

	private final static String TABLE_NAME = "TABLE_NAME";
	private final static String COLUMN_NAME = "COLUMN_NAME";
	private final static String TYPE_NAME = "TYPE_NAME";
	private final static String DATA_TYPE = "DATA_TYPE";
	private final static String DECIMAL_DIGITS = "DECIMAL_DIGITS";
	private final static String NUM_PREC_RADIX = "NUM_PREC_RADIX";
	private final static String COLUMN_SIZE = "COLUMN_SIZE";
	private final static String PRI = "PRI";
	private final static String KEY_SEQ = "KEY_SEQ";
	private final static String HIVE_KEY_SEQ = "KEQ_SEQ";
	private final static String PK_TABLE_NAME = "PKTABLE_NAME";
	private final static String PK_COLUMN_NAME = "PKCOLUMN_NAME";
	private final static String FK_COLUMN_NAME = "FKCOLUMN_NAME";
	private final static String IS_NULLABLE = "IS_NULLABLE ";
	private final static String COLUMN_DEF = "COLUMN_DEF";
	private final static String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
	private final static String IS_AUTO_INCREMENT = "IS_AUTO_INCREMENT";
	public final static String IS_AUTOINCREMENT_YES = "YES";
	public final static String IS_AUTOINCREMENT_NO = "NO";

	public static final String VIEW_TABLE_TYPE = "VIEW";
	public static final String FUNCTION_TABLE_TYPE = "FUNCTION";
	public static final String PROCEDURE_TABLE_TYPE = "PROCEDURE";
	public static final String TABLE_TABLE_TYPE = "TABLE";
	public static final String INDEX_TABLE_TYPE = "INDEX";

	private static final String ORACLE_PG_TABLE_TEMPLATE = "%s.\"%s\"";
	private static final String MSSQL_TABLE_TEMPLATE = "%s.%s.[%s]";
	private static final String DEFAULT_TABLE_TEMPLATE = "%s.%s";
	private static final String HANA_TABLE_TEMPLATE = "\"%s\".\"%s\"";
	private static final String MYSQL_TABLE_TEMPLATE = "`%s`.`%s`";
	private static final String SYBASE_ASE_TABLE_TEMPLATE = "%s.%s.[%s]";
	private static final String GAUSS200_TABLE_TEMPLATE = "\"%s\".\"%s\"";

	private static final String ORACLE_FIELD_TEMPLATE = "\"%s\"";
	private static final String MSSQL_FIELD_TEMPLATE = "[%s]";
	private static final String MYSQL_FIELD_TEMPLATE = "`%s`";
	private static final String PGSQL_FIELD_TEMPLATE = "\"%s\"";
	private static final String HANA_FIELD_TEMPLATE = "\"%s\"";
	private static final String GAUSS200_FIELD_TEMPLATE = "\"%s\"";
	private static final String CLICKHOUSE_FIELD_TEMPLATE = "`%s`";

	private static final String INSERT_PREPARE_STMT_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)";
	private static final String UPDATE_PREPARE_STMT_SQL_TEMPLATE = "UPDATE %s SET %s WHERE %s";
	private static final String DELETE_PREPARE_STMT_SQL_TEMPLATE = "DELETE FROM %s WHERE %s";
	private static final String CHECK_EXISTS_PREPARE_STMT_SQL_TEMPLATE = "SELECT COUNT(1) as counts FROM %s WHERE %s";
	private static final String GET_ROW_PREPARE_STMT_SQL_TEMPLATE = "SELECT * FROM %s WHERE %s";
	private static final String TABLE_TOTAL_COUNT_SQL = "SELECT COUNT(1) as counts FROM %s";

	private final static String FIND_DATA_BASIC_SQL = "SELECT * FROM %s.\"%s\" WHERE 1=1 ";
	private final static String FIND_DATA_BASIC_SQL_MYSQL = "SELECT * FROM %s.%s WHERE 1=1 ";
	private final static String FIND_DATA_BASIC_SQL_MSSQL = "SELECT * FROM %s.%s.[%s] WHERE 1=1 ";
	private final static String FIND_DATA_BASIC_SQL_SYBASE_ASE = "SELECT * FROM %s.%s.[%s] WHERE 1=1 ";

	private final static String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE %s";
	private final static String DELETE_TABLE_SQL = "DELETE FROM %s";

	public static final String PROCEDURE_NOT_SUPPORT = "not support";

	// https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPTZ
	private static final int TIMESTAMP_TZ_TYPE = -101;
	// https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPLTZ
	private static final int TIMESTAMP_LTZ_TYPE = -102;

	// mysql与mariadb的特有类型
	private static final int MYSQL_YEAR_TYPE = -999;

	protected static final Logger logger = LogManager.getLogger(JdbcUtil.class);

	public static void closeQuietly(AutoCloseable c) {
		try {
			if (null != c) {
				c.close();
			}
		} catch (Throwable ignored) {
		}
	}

	public static ResultSet getTableMetadata(
			Connection connection,
			String catalog,
			String schemaPattern,
			String schemaLessTablePattern,
			boolean includeViews
	) throws SQLException {
		return connection.getMetaData().getTables(
				catalog,
				schemaPattern,
				schemaLessTablePattern,
				includeViews ? new String[]{"TABLE", "VIEW"} : new String[]{"TABLE"}
		);
	}

	public static ResultSet getColumnMetadata(Connection connection, String catalog, String schema, String tableName, String columnPattern) throws SQLException {
		DatabaseMetaData metadata = connection.getMetaData();
		return metadata.getColumns(catalog, schema, tableName, columnPattern); // Get all columns for this table
	}

	public static ResultSet getPrimaryKeysResultSet(Connection connection, String schema, String tableName, String catalog) throws SQLException {
		DatabaseMetaData metadata = connection.getMetaData();
		return metadata.getPrimaryKeys(catalog, schema, tableName);
	}

	public static ResultSet getReferredTablesResultSet(Connection connection, String schema, String tableName, String catalog) throws SQLException {
		DatabaseMetaData metadata = connection.getMetaData();
		return metadata.getImportedKeys(catalog, schema, tableName);
	}

	private static void setPrimaryKey(RelateDatabaseField field, Map<String, Integer> map) {
		if (field != null && map != null) {
			if (map.containsKey(field.getField_name())) {
				field.setKey(PRI);
				field.setPrimary_key_position(map.get(field.getField_name()));
			}
		}
	}

	private static void setForeignKey(RelateDatabaseField field, Map<String, SchemaBean> map) {
		if (field != null && map != null) {
			if (map.containsKey(field.getField_name())) {
				SchemaBean schemaBean = map.get(field.getField_name());
				field.setForeign_key_column(schemaBean.getPk_column_name());
				field.setForeign_key_table(schemaBean.getPk_table_name());
			}
		}
	}

	public static Map<String, Integer> getPkMap(Connection conn, String databaseType, String databaseName,
												String schema, String tableName, Predicate<?> stop) throws SQLException {
		Map<String, Integer> pkMap = new HashMap<>();
		ResultSet pkRs = null;
		try {
			if (databaseType.equalsIgnoreCase("hive")) {
				if (databaseName != null) {
					databaseName = databaseName.toLowerCase();
				}
				tableName = tableName.toLowerCase();
				// hive use database name as schema when get pk
				pkRs = JdbcUtil.getPrimaryKeysResultSet(conn, databaseName, tableName, databaseName);
				hivePkResultSetToMap(pkRs, pkMap, stop);
			} else {
				pkRs = JdbcUtil.getPrimaryKeysResultSet(conn, schema, tableName, databaseName);
				pkResultsetToMap(pkRs, pkMap, stop);
			}
		} finally {
			closeQuietly(pkRs);
		}
		return pkMap;
	}

	private static void pkResultsetToMap(ResultSet rs, Map<String, Integer> map, Predicate<?> stop) throws SQLException {
		if (rs != null) {
			while (rs.next()) {
				if (null != stop && stop.test(null)) {
					break;
				}
				map.put(rs.getString(COLUMN_NAME), rs.getInt(KEY_SEQ));
			}
		}
	}

	private static void hivePkResultSetToMap(ResultSet rs, Map<String, Integer> map, Predicate<?> stop) throws SQLException {
		if (rs != null) {
			while (rs.next()) {
				if (null != stop && stop.test(null)) {
					break;
				}
				map.put(rs.getString(COLUMN_NAME), rs.getInt(HIVE_KEY_SEQ));
			}
		}
	}

	private static void fkResultsetToMap(ResultSet rs, Map<String, SchemaBean> map) throws SQLException {
		if (rs != null) {
			while (rs.next()) {
				map.put(rs.getString(FK_COLUMN_NAME), new SchemaBean(
						rs.getString(PK_COLUMN_NAME),
						rs.getString(PK_TABLE_NAME)
				));
			}
		}
	}

	public static String formatTableName(String databaseName, String schema, String tableName, String databaseType) {

//        if (!caseSensitive(databaseType)) {
//            tableName = tableName.toUpperCase();
//        }
		DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(databaseType);
		switch (databaseTypeEnum) {
			case ORACLE:
			case DAMENG:
			case POSTGRESQL:
			case ALIYUN_POSTGRESQL:
			case ADB_POSTGRESQL:
			case GREENPLUM:
				tableName = String.format(ORACLE_PG_TABLE_TEMPLATE, schema, tableName);
				break;
			case MSSQL:
			case ALIYUN_MSSQL:
				tableName = String.format(MSSQL_TABLE_TEMPLATE, databaseName, schema, tableName);
				break;
			case SYBASEASE:
				tableName = String.format(SYBASE_ASE_TABLE_TEMPLATE, databaseName, schema, tableName);
				break;
			case GAUSSDB200:
				tableName = String.format(GAUSS200_TABLE_TEMPLATE, schema, tableName);
				break;
			case MYSQL:
			case MARIADB:
			case MYSQL_PXC:
			case TIDB:
			case KUNDB:
			case ADB_MYSQL:
			case ALIYUN_MYSQL:
			case ALIYUN_MARIADB:
				tableName = String.format(MYSQL_TABLE_TEMPLATE, databaseName, tableName);
				break;
			case HIVE:
				tableName = String.format(DEFAULT_TABLE_TEMPLATE, databaseName.toLowerCase(), tableName.toLowerCase());
				break;
			case HANA:
				tableName = String.format(HANA_TABLE_TEMPLATE, schema, tableName);
				break;
			default:
				tableName = String.format(DEFAULT_TABLE_TEMPLATE, databaseName, tableName);
		}

		return tableName;
	}

	public static String formatFieldName(String fieldName, String databaseType) {

		DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(databaseType);
		switch (databaseTypeEnum) {
			case ORACLE:
			case DAMENG:
				fieldName = String.format(ORACLE_FIELD_TEMPLATE, fieldName);
				break;
			case MSSQL:
			case ALIYUN_MSSQL:
				fieldName = String.format(MSSQL_FIELD_TEMPLATE, fieldName);
				break;
			case MYSQL:
			case MARIADB:
			case MYSQL_PXC:
			case TIDB:
			case KUNDB:
			case ADB_MYSQL:
			case ALIYUN_MYSQL:
			case ALIYUN_MARIADB:
				fieldName = String.format(MYSQL_FIELD_TEMPLATE, fieldName);
				break;
			case HIVE:
				fieldName = String.format(MYSQL_FIELD_TEMPLATE, fieldName.toLowerCase());
				break;
			case HANA:
				fieldName = String.format(HANA_FIELD_TEMPLATE, fieldName);
				break;
			case POSTGRESQL:
			case ALIYUN_POSTGRESQL:
			case ADB_POSTGRESQL:
			case GREENPLUM:
				fieldName = String.format(PGSQL_FIELD_TEMPLATE, fieldName);
				break;
			case GAUSSDB200:
				fieldName = String.format(GAUSS200_FIELD_TEMPLATE, fieldName);
				break;
			case CLICKHOUSE:
				fieldName = String.format(CLICKHOUSE_FIELD_TEMPLATE, fieldName);
				break;
			default:
				break;
		}

		return fieldName;
	}
}
