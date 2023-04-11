package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

public class MyBinaryLogClient extends BinaryLogClient {

    public MyBinaryLogClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
    }
}
