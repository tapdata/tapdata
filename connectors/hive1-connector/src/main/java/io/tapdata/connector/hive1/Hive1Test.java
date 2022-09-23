package io.tapdata.connector.hive1;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.hive1.config.Hive1Config;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;

import java.sql.Connection;

import static io.tapdata.base.ConnectorBase.testItem;


public class Hive1Test extends CommonDbTest {

    public static final String TAG = Hive1Test.class.getSimpleName();

    public Hive1Test(Hive1Config hive1Config) {
        super(hive1Config);
        try {
            jdbcContext = (Hive1JdbcContext) DataSourcePool.getJdbcContext(hive1Config, Hive1JdbcContext.class, uuid);
        } catch (Exception e) {
            TapLogger.error(TAG,"create Hive1JdbcContext error:{}",e.getMessage());
            throw new RuntimeException(e);
        }

    }

    public TestItem testConnect(Hive1Config hive1Config) {
        try (
                Connection connection = ((Hive1JdbcContext)jdbcContext).getConnection(hive1Config)
        ) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testConnectOfStream(Hive1Config hive1Config) {
        IMetaStoreClient metaStoreClient = null;
        try {
            metaStoreClient = new HiveMetaStoreClient(newHiveConf(hive1Config));
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }finally {
            if(metaStoreClient!=null) metaStoreClient.close();
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

}
