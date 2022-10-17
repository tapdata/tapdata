package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

import static io.tapdata.base.ConnectorBase.testItem;


/**
 * @author lemon
 */
public class TidbConnectionTest extends CommonDbTest {

	private  TidbConfig tidbConfig;

	private final static String PB_SERVER_SUCESS = "Check PDServer  host port is invalid";

	public TidbConnectionTest(TidbConfig tidbConfig) {
		super(tidbConfig);
		this.tidbConfig = tidbConfig;
		jdbcContext = DataSourcePool.getJdbcContext(tidbConfig, TidbContext.class, uuid);
	}


	public TestItem testConnect() {
		try (
				Connection connection = jdbcContext.getConnection();
		) {
			return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
		}catch (Exception e) {
			if (e instanceof SQLException) {
				String errMsg = e.getMessage();
				if (errMsg.contains("using password")) {
					String password =commonDbConfig.getPassword();
					if (StringUtils.isNotEmpty(password)) {
						errMsg = "password or username is error ,please check";
					} else {
						errMsg = "password is empty,please enter password";
					}
					return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, errMsg);

				}
			}
			return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
		}
	}

	/**
	 * check Pbserver
	 * @return
	 */
	public TestItem testPbserver() {
		URI uri = URI.create(tidbConfig.getPdServer());
		try {
			NetUtil.validateHostPortWithSocket(uri.getHost(), uri.getPort());
			return testItem(PB_SERVER_SUCESS, TestItem.RESULT_SUCCESSFULLY);
		} catch (IOException e) {
			return testItem(PB_SERVER_SUCESS, TestItem.RESULT_FAILED, e.getMessage());
		}

	}

}
