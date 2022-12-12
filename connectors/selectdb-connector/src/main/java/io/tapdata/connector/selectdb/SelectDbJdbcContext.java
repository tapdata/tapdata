package io.tapdata.connector.selectdb;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.entity.utils.DataMap;
import java.util.List;

/**
 * Author:Skeet
 * Date: 2022/12/8 16:24
 **/
public class SelectDbJdbcContext extends JdbcContext {
    private static final String TAG = SelectDbJdbcContext.class.getSimpleName();

    public SelectDbJdbcContext(SelectDbConfig config, HikariDataSource hikariDataSource) {
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