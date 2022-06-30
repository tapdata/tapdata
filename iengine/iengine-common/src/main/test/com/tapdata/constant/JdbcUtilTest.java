package com.tapdata.constant;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Mapping;
import org.bson.Document;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@Ignore
public class JdbcUtilTest {

	Connections connections = null;

	@Before
	public void init() {
		connections = new Connections();
		connections.setDatabase_host("127.0.0.1");
		connections.setDatabase_port(3306);
		connections.setDatabase_name("inventory");
		connections.setDatabase_username("root");
		connections.setDatabase_password("tapdata");
	}

	@Test
	public void getTableMetadataTest() throws Exception {
		try (
				Connection conn = MySqlUtil.createMySQLConnection(connections);
				ResultSet test = JdbcUtil.getTableMetadata(conn, connections.getDatabase_name(),
						"%", "test1", false)
		) {
			List<Document> list = new ArrayList<>();
			while (test.next()) {
				Document doc = new Document();
				for (int i = 0; i < test.getMetaData().getColumnCount(); i++) {
					doc.append(test.getMetaData().getColumnName(i + 1), test.getObject(i + 1));
				}
				list.add(doc);
			}

			list.forEach(document -> System.out.println(document));
		}
	}

	@Test
	public void ddl() {

		Mapping mapping = new Mapping();
		mapping.setTo_table("AA");
		mapping.setFieldsNameTransform("toLowerCase");
		String ddlSQL = JdbcUtil.formatDDLSQL(
				"ALTER TABLE `INSURANCE`.`POLICY` MODIFY POLICY_STATUS varchar (741) null;",
				mapping,
				"db",
				"dba",
				"mysql",
				true,
				DatabaseTypeEnum.MYSQL
		);

		System.out.println(ddlSQL);

	}
}
