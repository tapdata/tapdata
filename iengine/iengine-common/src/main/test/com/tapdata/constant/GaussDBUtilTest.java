package com.tapdata.constant;

import com.tapdata.entity.Connections;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class GaussDBUtilTest {

	private Connections connections;

	@Before
	public void before() {
		connections = new Connections();
		connections.setDatabase_host("119.3.31.31");
		connections.setDatabase_port(9000);
		connections.setDatabase_name("tapdata");
		connections.setDatabase_username("dbadmin");
		connections.setDatabase_password("Beidou0cha!");
	}

	@Test
	public void createConnectionTest() {
		try (
				Connection conn = GaussDBUtil.createConnection(connections)
		) {
			assertNotNull(conn);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
