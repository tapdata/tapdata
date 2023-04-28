package io.tapdata.connector.doris;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class DorisTest extends CommonDbTest {

    public DorisTest(DorisConfig dorisConfig, Consumer<TestItem> consumer) {
        super(dorisConfig, consumer);
        jdbcContext = new DorisJdbcContext(dorisConfig);
    }

    @Override
    protected Boolean testVersion() {
        try {
            String dorisVersion = jdbcContext.queryVersion();
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, dorisVersion));
        } catch (Throwable throwable) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, throwable.getMessage()));
        }
        return true;
    }

    @Override
    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("`" + commonDbConfig.getSchema() + "`.") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_DORIS_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_DORIS_WRITE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_DORIS_UPDATE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_DORIS_DELETE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    protected static final String TEST_DORIS_CREATE_TABLE = "create table %s(col1 int not null, col2 int) unique key(col1) distributed by hash(col1) buckets 2 PROPERTIES(\"replication_num\"=\"1\")";
    protected static final String TEST_DORIS_WRITE_RECORD = "insert into %s values(0,0)";
    protected static final String TEST_DORIS_UPDATE_RECORD = "update %s set col2=1 where col1=0";
    protected static final String TEST_DORIS_DELETE_RECORD = "delete from %s where col1=0";
}
