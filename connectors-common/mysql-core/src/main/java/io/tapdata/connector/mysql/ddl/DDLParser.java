package io.tapdata.connector.mysql.ddl;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 14:27
 **/
public interface DDLParser<T> {
	T parse(String ddl) throws Throwable;
}
