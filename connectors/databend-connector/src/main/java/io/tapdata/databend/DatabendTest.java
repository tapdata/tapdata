package io.tapdata.databend;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.pdk.apis.entity.TestItem;

import static io.tapdata.base.ConnectorBase.testItem;

import io.tapdata.databend.config.DatabendConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;


public class DatabendTest extends CommonDbTest {

    public DatabendTest(DatabendConfig databendConfig, Consumer<TestItem> consumer) {
        super(databendConfig, consumer);
        jdbcContext = DataSourcePool.getJdbcContext(databendConfig, DatabendJdbcContext.class, uuid);
    }

    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_CK_CREATE_TABLE, TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_CK_WRITE_RECORD, TEST_WRITE_TABLE));
            //update
//            sqls.add(String.format(TEST_CK_UPDATE_RECORD, TEST_WRITE_TABLE));
            //delete
//            sqls.add(String.format(TEST_CK_DELETE_RECORD, TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    private static final String TEST_CK_CREATE_TABLE = "create table %s(col1 int,col2 int)";
    private static final String TEST_CK_WRITE_RECORD = "insert into %s values(1,2)";
    //TODO: wait new databend version that support update and alter
//    private static final String TEST_CK_UPDATE_RECORD = "alter table %s update col2=1 where 1=1";
//    private static final String TEST_CK_DELETE_RECORD = "alter table %s delete where 1=1";
}