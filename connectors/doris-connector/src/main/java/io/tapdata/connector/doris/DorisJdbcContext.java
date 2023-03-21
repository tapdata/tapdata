package io.tapdata.connector.doris;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.JdbcContext;
import io.tapdata.entity.utils.DataMap;

import java.util.List;

public class DorisJdbcContext extends JdbcContext {
    public DorisJdbcContext(CommonDbConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        return null;
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        return null;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        return null;
    }
}
