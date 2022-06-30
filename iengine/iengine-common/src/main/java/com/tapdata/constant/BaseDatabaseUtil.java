package com.tapdata.constant;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.FieldProcess;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.exception.BaseDatabaseUtilException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

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

}
