package io.tapdata.oracle;

import com.tapdata.entity.Connections;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Ignore
public class CreateBigTableTest {

	final static String host = "127.0.0.1";
	final static int port = 2521;
	final static String userName = "TAPDATA";
	final static String password = "Gotapd8!";
	final static String databaseName = "XE";
	final static String owner = "TAPDATA";
	final static String tableName = "BIG_TABLE";

	Connections connections;
	int columnsCount = 300;
	List<String> baseTypes = new ArrayList<>();

	@Before
	public void init() {
		baseTypes.add(String.format("NVARCHAR2(50) DEFAULT '%s'", RandomStringUtils.randomAlphabetic(30)));
		baseTypes.add(String.format("CHAR(5) DEFAULT '%s'", RandomStringUtils.randomAlphabetic(5)));
		baseTypes.add(String.format("NUMBER DEFAULT %s", RandomUtils.nextInt(1, 100)));

		connections = new Connections();
		connections.setDatabase_host(host);
		connections.setDatabase_port(port);
		connections.setDatabase_username(userName);
		connections.setDatabase_password(password);
		connections.setDatabase_name(databaseName);
		connections.setDatabase_owner(owner);
	}

	@Test
	public void createTable() throws Exception {
		StringBuilder createSqlBuilder = new StringBuilder(String.format("CREATE TABLE %s.%s(\n", owner, tableName))
				.append("  ID NUMBER PRIMARY KEY,\n");
		StringBuilder insertSqlBuilder = new StringBuilder(String.format("INSERT INTO %s.%s(ID) VALUES(%s)",
				owner, tableName, RandomUtils.nextInt()));

		IntStream.range(0, columnsCount).forEach(n -> {
			String type = baseTypes.get(RandomUtils.nextInt(0, baseTypes.size()));
			createSqlBuilder.append("  COLUMN" + (n + 1) + " " + type);
			if (n == columnsCount - 1) {
				createSqlBuilder.append("\n)");
			} else {
				createSqlBuilder.append(",\n");
			}
		});
	}
}
