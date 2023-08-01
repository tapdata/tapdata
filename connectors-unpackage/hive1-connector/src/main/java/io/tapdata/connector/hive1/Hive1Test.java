package io.tapdata.connector.hive1;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.connector.hive1.dml.Hive1Writer;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;


public class Hive1Test extends CommonDbTest {

    public Hive1Test(Hive1Config hive1Config, Consumer<TestItem> consumer) {
        super(hive1Config, consumer);
        jdbcContext = new Hive1JdbcContext(hive1Config);
    }

    @Override
    public Boolean testOneByOne() {
        if (isStreamConnection((Hive1Config) commonDbConfig)) {
            testFunctionMap.put("testConnectOfStream", this::testConnectOfStream);
        }
        return super.testOneByOne();
    }

    public boolean testConnectOfStream() {
        IMetaStoreClient metaStoreClient = null;
        try {
            metaStoreClient = new HiveMetaStoreClient(newHiveConf((Hive1Config) commonDbConfig));
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        } finally {
            if (metaStoreClient != null) metaStoreClient.close();
        }
    }

    private HiveConf newHiveConf(Hive1Config hive1Config) throws Exception {
        HiveConf conf = new HiveConf();
//        conf.set("fs.raw.impl", Hive1RawFileSystem.class.getName());
        conf.setVar(HiveConf.ConfVars.METASTOREURIS, getMetastoreUri(hive1Config));
//        conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, true);
//        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);

        // prepare transaction database
//        TxnDbUtil.setConfValues(conf);
//        TxnDbUtil.cleanDb();
//        TxnDbUtil.prepDb();
//
        return conf;
    }

    private String getMetastoreUri(Hive1Config hive1Config) {
        return String.format("thrift://%s:%d", hive1Config.getHost(), hive1Config.getPort());
    }

    private boolean isStreamConnection(Hive1Config hive1Config) {
        String hiveConnType = hive1Config.getHiveConnType();
        if (StringUtils.isNotBlank(hiveConnType) && Hive1Writer.HIVE_STREAM_CONN.equals(hiveConnType))
            return true;
        return false;
    }

}
