package io.tapdata.connector.gbase8s;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.gbase8s.config.Gbase8sConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

public class Gbase8sTest extends CommonDbTest {

    public Gbase8sTest(Gbase8sConfig gbase8sConfig, Consumer<TestItem> consumer) {
        super(gbase8sConfig, consumer);
        jdbcContext = new Gbase8sJdbcContext(gbase8sConfig);
    }

}
