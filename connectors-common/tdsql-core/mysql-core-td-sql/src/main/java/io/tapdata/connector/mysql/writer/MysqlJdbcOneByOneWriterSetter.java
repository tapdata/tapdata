package io.tapdata.connector.mysql.writer;

public interface MysqlJdbcOneByOneWriterSetter{
	public MysqlJdbcOneByOneWriter set(Object jdbcCacheMap) throws Throwable;
}