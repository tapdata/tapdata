package com.tapdata.validator;

import com.mongodb.MongoClient;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.*;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.ConverterUtil;
import io.tapdata.schema.SchemaProxy;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SchemaFactory {

	final static String TABLE_NAME = "TABLE_NAME";
	final static String COLUMN_NAME = "COLUMN_NAME";
	final static String TYPE_NAME = "TYPE_NAME";
	final static String DATA_TYPE = "DATA_TYPE";
	final static String DECIMAL_DIGITS = "DECIMAL_DIGITS";
	final static String NUM_PREC_RADIX = "NUM_PREC_RADIX";
	final static String COLUMN_SIZE = "COLUMN_SIZE";
	final static String PRI = "PRI";
	final static String KEY_SEQ = "KEY_SEQ";
	final static String HIVE_KEY_SEQ = "KEQ_SEQ";

	final static String PK_TABLE_NAME = "PKTABLE_NAME";
	final static String PK_COLUMN_NAME = "PKCOLUMN_NAME";
	final static String FK_COLUMN_NAME = "FKCOLUMN_NAME";
	final static String IS_NULLABLE = "IS_NULLABLE";
	final static String COLUMN_DEF = "COLUMN_DEF";
	final static String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
	// Hive use `IS_AUTO_INCREMENT` instead of `IS_AUTOINCREMENT`
	final static String IS_AUTO_INCREMENT = "IS_AUTO_INCREMENT";
	final static String REMARKS = "REMARKS";
	final static String ORDINAL_POSITION = "ORDINAL_POSITION";

	private final static String SCHEMAVALIDATORIMPL = "SchemaValidatorImpl";
	private final static String VALIDATE_SCHEMA = "validateSchema";
	private final static String LOAD_SCHEMA_ERROR = SCHEMAVALIDATORIMPL + "." + VALIDATE_SCHEMA + "failed, database \n %s";
	private final static String CONN_ERROR = "Try connect to database failed,please check your connections\n %s";
	private final static String UNSUPPORT_DATABASE_TYPE = "Unsupport database type: %s";

	public static Schema loadSchemaList(Connections connections, boolean loadFields) throws Exception {

		List<RelateDataBaseTable> relateDataBaseTables;
		Schema schema = null;
		SchemaValidator schemaValidator = null;

		try {
			if (StringUtils.isNotBlank(connections.getDatabase_type())) {

				schemaValidator = getSchemaValidator(connections);

				if (schemaValidator != null && !schemaValidator.isEmpty()) {
					if (loadFields) {
						relateDataBaseTables = schemaValidator.getSchemaValidator().validateSchema(connections, schemaValidator.getConn());
						schema = new Schema(relateDataBaseTables);
					} else {
						try {
							relateDataBaseTables = schemaValidator.getSchemaValidator().loadSchemaTablesOnly(connections, schemaValidator.getConn());
							schema = new Schema(relateDataBaseTables, false);
						} catch (UnsupportedOperationException e) {
							relateDataBaseTables = schemaValidator.getSchemaValidator().validateSchema(connections, schemaValidator.getConn());
							schema = new Schema(relateDataBaseTables);
						}
					}
				}
			}
		} finally {
			releaseConn(schemaValidator);
		}

		return schema;
	}

	public static void loadSchemaList(Connections connections, Consumer<RelateDataBaseTable> tableConsumer) throws Exception {
		SchemaValidator schemaValidator = null;

		try {
			schemaValidator = getSchemaValidator(connections);

			if (schemaValidator != null && !schemaValidator.isEmpty()) {
				schemaValidator.getSchemaValidator().validateSchema(connections, schemaValidator.getConn(), tableConsumer);
			}
		} finally {
			releaseConn(schemaValidator);
		}
	}

	public static RelateDataBaseTable updateSchema(ClientMongoOperator clientMongoOperator, Connections connection, String tableName) throws Exception {
		RelateDataBaseTable table = null;
		if (SchemaFactory.canLoad(connection)) {
			connection.setTable_filter(tableName);
			connection.setLoadSchemaField(true);
			connection.setFile_schema(tableName);
			Schema schema = SchemaFactory.loadSchemaList(connection, true);
			if (null != schema) {
				List<RelateDataBaseTable> tables = schema.getTables();
				if (null != tables && !tables.isEmpty()) {
					ConverterUtil.schemaConvert(tables, connection.getDatabase_type());
					table = tables.get(0);
					clientMongoOperator.update(
							Query.query(Criteria.where("_id").is(connection.getId()))
							, Update.update("schema.tables", tables).set("tableCount", 1).set("loadCount", 1)
							, ConnectorConstant.CONNECTION_COLLECTION);
				}
			}
			SchemaProxy.getSchemaProxy().clear(connection.getId());
		}
		return table;
	}

	private static void releaseConn(SchemaValidator schemaValidator) {
		if (schemaValidator != null) {
			if (schemaValidator.getConn() instanceof Connection) {
//        JdbcUtil.closeQuietly((Connection) schemaValidator.getConn());
			} else if (schemaValidator.getConn() instanceof MongoClient) {
				if (schemaValidator.getConn() != null) {
					((MongoClient) schemaValidator.getConn()).close();
				}
			}
		}
	}

	public static SchemaValidator getSchemaValidator(Connections connections) throws Exception {
		ISchemaValidator iSchemaValidator = null;
		Object conn = null;

//    switch (DatabaseTypeEnum.fromString(connections.getDatabase_type())) {
//      case ORACLE:
//        conn = OracleUtil.createConnection(connections);
//        iSchemaValidator = new OracleSchemaValidator();
//        break;
//      case DAMENG:
//        conn = MySqlUtil.createDmConnection(connections);
//        iSchemaValidator = new MysqlSchemaValidatorImpl();
//        break;
//      case MYSQL:
//      case MARIADB:
//      case MYSQL_PXC:
//      case HANA:
//      case KUNDB:
//      case ALIYUN_MYSQL:
//      case ALIYUN_MARIADB:
//        conn = MySqlUtil.createMySQLConnection(connections);
//        iSchemaValidator = new MysqlSchemaValidatorImpl();
//        break;
//      case ADB_MYSQL:
//        conn = MySqlUtil.createDmConnection(connections);
//        iSchemaValidator = new ADBMysqlSchemaValidatorImpl();
//        break;
//      case MSSQL:
//      case ALIYUN_MSSQL:
//        conn = MsSqlUtil.createConnection(connections);
//        iSchemaValidator = new MsSqlSchemaValidatorImpl();
//        break;
//      case SYBASEASE:
//        conn = SybaseUtil.createConnection(connections);
//        iSchemaValidator = new SybaseSchemaValidatorImpl();
//        break;
//      case MONGODB:
//      case ALIYUN_MONGODB:
//        conn = MongodbUtil.createMongoClient(connections);
//        iSchemaValidator = new MongodbSchemaValidatorImpl();
//        break;
//      default:
//        break;
//    }

		return new SchemaValidator(iSchemaValidator, conn);
	}

	public static boolean canLoad(Connections connections) throws Exception {
		SchemaValidator schemaValidator = null;
		try {
			schemaValidator = getSchemaValidator(connections);
		} finally {
			releaseConn(schemaValidator);
		}
		return schemaValidator.getSchemaValidator() != null;
	}

	static void setPrimaryKey(RelateDatabaseField field, Map<String, Integer> map) {
		if (field != null && map != null) {
			if (map.containsKey(field.getField_name())) {
				field.setKey(PRI);
				field.setPrimary_key_position(map.get(field.getField_name()));
			}
		}
	}

	static void setForeignKey(RelateDatabaseField field, Map<String, SchemaBean> map) {
		if (field != null && map != null) {
			if (map.containsKey(field.getField_name())) {
				SchemaBean schemaBean = map.get(field.getField_name());
				field.setForeign_key_column(schemaBean.getPk_column_name());
				field.setForeign_key_table(schemaBean.getPk_table_name());
			}
		}
	}

	static void pkResultsetToMap(ResultSet rs, Map<String, Integer> map) throws SQLException {
		if (rs != null) {
			while (rs.next()) {
				map.put(rs.getString(COLUMN_NAME), rs.getInt(KEY_SEQ));
			}
		}
	}

	static void hivePkResultSetToMap(ResultSet rs, Map<String, Integer> map) throws SQLException {
		if (rs != null) {
			while (rs.next()) {
				map.put(rs.getString(COLUMN_NAME), rs.getInt(HIVE_KEY_SEQ));
			}
		}
	}

	static void fkResultsetToMap(ResultSet rs, Map<String, SchemaBean> map) throws SQLException {
		if (rs != null) {
			while (rs.next()) {
				map.put(rs.getString(FK_COLUMN_NAME), new SchemaBean(
						rs.getString(PK_COLUMN_NAME),
						rs.getString(PK_TABLE_NAME)
				));
			}
		}
	}

	public static class SchemaValidator {
		private ISchemaValidator iSchemaValidator;
		private Object conn;

		public SchemaValidator(ISchemaValidator iSchemaValidator, Object conn) {
			this.iSchemaValidator = iSchemaValidator;
			this.conn = conn;
		}

		public ISchemaValidator getSchemaValidator() {
			return iSchemaValidator;
		}

		public Object getConn() {
			return conn;
		}

		public boolean isEmpty() {
			return iSchemaValidator == null || conn == null;
		}
	}
}
