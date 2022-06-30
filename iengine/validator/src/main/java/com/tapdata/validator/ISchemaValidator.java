package com.tapdata.validator;

import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDataBaseTable;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public interface ISchemaValidator {

	/**
	 * load database schema
	 *
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	List<RelateDataBaseTable> validateSchema(Connections conn) throws Exception;

	<E> List<RelateDataBaseTable> validateSchema(Connections connections, E connection) throws Exception;

	<E> void validateSchema(Connections connections, E connection, Consumer<RelateDataBaseTable> tableConsumer) throws Exception;

	default <E> void validateSchema(Connections connections, E connection, String schemaPattern, Consumer<RelateDataBaseTable> tableConsumer) throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * Only load schema tables, not include fields
	 *
	 * @param connections
	 * @param connection
	 * @return
	 * @throws Exception
	 */
	default <E> List<RelateDataBaseTable> loadSchemaTablesOnly(Connections connections, E connection) throws Exception {
		throw new UnsupportedOperationException();
	}
}
