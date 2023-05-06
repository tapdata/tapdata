package io.tapdata.connector.doris;

import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jarad
 * @date 7/14/22
 */
public class DorisJdbcContext extends MysqlJdbcContextV2 {

    public DorisJdbcContext(DorisConfig dorisConfig) {
        super(dorisConfig);
    }

    public String queryVersion() throws SQLException {
        AtomicReference<String> version = new AtomicReference<>();
        queryWithNext(DORIS_VERSION, resultSet -> version.set(resultSet.getString("Value")));
        return version.get();
    }

    private static final String DORIS_VERSION = "show variables like '%version_comment%'";
}
