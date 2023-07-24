package io.tapdata.connector.gbase8s;

import com.gbasedbt.asf.Connection;
import com.gbasedbt.jdbcx.IfxConnectionPoolDataSource;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Author:Skeet
 * Date: 2023/6/26
 **/
public class GBase8sConnectionExample {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
            String url = "jdbc:gbasedbt-sqli://localhost:19088/zhtest:GBASEDBTSERVER=gbase01";
            String username = "gbasedbt";
            String password = "Gbase123";
            Class.forName("com.gbasedbt.jdbc.Driver");
            Connection conn = (Connection) DriverManager.getConnection(url,username,password);
    }
}
