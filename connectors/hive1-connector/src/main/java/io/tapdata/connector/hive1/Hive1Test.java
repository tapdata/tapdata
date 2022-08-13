package io.tapdata.connector.hive1;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.hive1.config.Hive1Config;


public class Hive1Test extends CommonDbTest {

    public Hive1Test(Hive1Config hive1Config) {
        super(hive1Config);
        jdbcContext = DataSourcePool.getJdbcContext(hive1Config, Hive1JdbcContext.class, uuid);

    }
}
