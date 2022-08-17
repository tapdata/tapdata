package io.tapdata.connector.hive1.config;

import org.apache.hive.jdbc.HiveConnection;

import java.sql.SQLException;
import java.util.Properties;

public class MyHive1Connection extends HiveConnection {
    public MyHive1Connection(String uri, Properties info) throws SQLException {
        super(uri, info);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this!=null;
    }
}
