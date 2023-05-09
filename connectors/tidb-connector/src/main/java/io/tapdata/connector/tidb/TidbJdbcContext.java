package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;

public class TidbJdbcContext extends MysqlJdbcContextV2 {

    public TidbJdbcContext(CommonDbConfig config) {
        super(config);
    }

}
