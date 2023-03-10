package io.tapdata.pdk.tdd.tests.support.connector;

import java.util.UUID;

public class MYSQLSupport implements TableNameSupport {
    @Override
    public String tableName() {
        return UUID.randomUUID().toString().replaceAll("-", "_");
    }
}
