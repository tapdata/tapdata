package io.tapdata.connector.dameng;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;

/**
 * @author lemon
 */
public class DamengTest extends CommonDbTest {
    public DamengTest(DamengConfig damengConfig) {
        super(damengConfig);
        jdbcContext = DataSourcePool.getJdbcContext(damengConfig, DamengContext.class, uuid);
    }
}
