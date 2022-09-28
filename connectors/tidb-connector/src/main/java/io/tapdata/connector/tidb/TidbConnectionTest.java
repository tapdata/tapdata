package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;


/**
 * @author lemon
 */
public class TidbConnectionTest extends CommonDbTest {

	public TidbConnectionTest(TidbConfig tidbConfig) {
		super(tidbConfig);
		jdbcContext = DataSourcePool.getJdbcContext(tidbConfig, TidbContext.class, uuid);
	}
}
