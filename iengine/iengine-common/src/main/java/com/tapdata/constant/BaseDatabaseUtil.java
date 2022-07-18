package com.tapdata.constant;

import com.tapdata.entity.Connections;
import com.tapdata.entity.FieldProcess;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.exception.BaseDatabaseUtilException;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public abstract class BaseDatabaseUtil {

	/**
	 * check index exists
	 *
	 * @param connections
	 * @param tableName
	 * @param fieldNames
	 * @return true - exists, false - not exists
	 * @throws BaseDatabaseUtilException
	 */
	public boolean isIndexExists(Connections connections, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {
		throw new BaseDatabaseUtilException("Not supported");
	}

	public boolean isIndexExists(ClientMongoOperator clientMongoOperator, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {
		throw new BaseDatabaseUtilException("Not supported");
	}

	public boolean isIndexExists(Connection connection, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {
		throw new BaseDatabaseUtilException("Not supported");
	}

	/**
	 * drop index
	 *
	 * @param clientMongoOperator
	 * @param tableName
	 * @param fieldNames
	 * @throws BaseDatabaseUtilException
	 */
	public void dropIndex(ClientMongoOperator clientMongoOperator, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {
		throw new BaseDatabaseUtilException("Not supported");
	}

	public void dropIndex(Connection connection, String tableName, List<String> fieldNames) throws BaseDatabaseUtilException {
		throw new BaseDatabaseUtilException("Not supported");
	}

	protected static String fieldProcessHandler(String fieldName, Map<String, FieldProcess> fieldProcessByFieldName, String databaseType) {
		String formatFieldName = "";
		if (MapUtils.isNotEmpty(fieldProcessByFieldName) && fieldProcessByFieldName.containsKey(fieldName)) {
			FieldProcess fieldProcess = fieldProcessByFieldName.get(fieldName);
			String fieldProcessOp = fieldProcess.getOp();
			FieldProcess.FieldOp fieldOp = FieldProcess.FieldOp.fromOperation(fieldProcessOp);
			if (fieldOp == FieldProcess.FieldOp.OP_RENAME) {
				formatFieldName = JdbcUtil.formatFieldName(fieldProcess.getOperand(), databaseType);
			} else if (fieldOp == FieldProcess.FieldOp.OP_REMOVE) {
				// do nothing
			}
		} else {
			formatFieldName = JdbcUtil.formatFieldName(fieldName, databaseType);
		}

		return formatFieldName;
	}

}
