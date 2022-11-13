package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;

import java.net.URI;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;


/**
 * @author lemon
 */
public class TidbConnectionTest extends CommonDbTest {

    private final TidbConfig tidbConfig;

    private final static String PB_SERVER_SUCCESS = "Check PDServer host port is valid";

    public TidbConnectionTest(TidbConfig tidbConfig, Consumer<TestItem> consumer) {
        super(tidbConfig, consumer);
        this.tidbConfig = tidbConfig;
        jdbcContext = DataSourcePool.getJdbcContext(tidbConfig, TidbContext.class, uuid);
    }

    @Override
    public Boolean testOneByOne() {
        testFunctionMap.put("testPbserver", this::testPbserver);
        return super.testOneByOne();
    }

    @Override
    public Boolean testReadPrivilege() {
        return true;
    }

    @Override
    public Boolean testStreamRead() {
        return true;
    }

    /**
     * check Pbserver
     *
     * @return
     */
    public Boolean testPbserver() {
        URI uri = URI.create(tidbConfig.getPdServer());
        try {
            NetUtil.validateHostPortWithSocket(uri.getHost(), uri.getPort());
            consumer.accept(testItem(PB_SERVER_SUCCESS, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Exception e) {
            consumer.accept(testItem(PB_SERVER_SUCCESS, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }

    }

}
