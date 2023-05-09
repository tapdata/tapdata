package io.tapdata.inspect.compare;

import com.tapdata.entity.Connections;
import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.ConverterProvider;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/18 1:09 下午
 * @description
 */
public class RdbmsResult extends BaseResult<Map<String, Object>> {
	List<String> columnNames;
	ResultSetMetaData metaData;
	HikariDataSource dataSource;
	Connection connection;
	Statement statement;
	ResultSet resultSet;
	int columnCount;

	public RdbmsResult(List<String> sortColumns, Connections connections, String tableName, ConverterProvider converterProvider) {
		super(sortColumns, connections, tableName, converterProvider);
	}

	@Override
	public void close() {
	}

	@Override
	public boolean hasNext() {
		try {
			return resultSet.next();
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
		return false;
	}

	@SneakyThrows
	@Override
	public Map<String, Object> next() {
		pointer++;
		Map<String, Object> value = new HashMap<>();
		return value;
	}

	@Override
	public long getTotal() {
		return total;
	}

	@Override
	public long getPointer() {
		return pointer;
	}
}
