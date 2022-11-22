package io.tapdata.oceanbase;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.oceanbase.connector.OceanbaseJdbcContext;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

public class OceanbaseTest extends CommonDbTest implements AutoCloseable {

    public OceanbaseTest(OceanbaseConfig oceanbaseConfig, Consumer<TestItem> consumer) {
        super(oceanbaseConfig, consumer);
        jdbcContext = DataSourcePool.getJdbcContext(oceanbaseConfig, OceanbaseJdbcContext.class, uuid);
    }

    @Override
    public Boolean testWritePrivilege() {
        return true;
    }

    @Override
    public Boolean testReadPrivilege() {
        return true;
    }

    @Override
    public Boolean testStreamRead() {
        return true;
    }
}


