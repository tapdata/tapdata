package io.tapdata.connector.greenplum;

import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.PostgresTest;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class GreenplumTest extends PostgresTest {

    public GreenplumTest(PostgresConfig postgresConfig, Consumer<TestItem> consumer) {
        super(postgresConfig, consumer);
        jdbcContext = new PostgresJdbcContext(postgresConfig);
    }

    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_GP_CREATE_TABLE, TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_GP_WRITE_RECORD, TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_GP_UPDATE_RECORD, TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_GP_DELETE_RECORD, TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    private static final String TEST_GP_CREATE_TABLE = "create table %s(col1 int,col2 int)";
    private static final String TEST_GP_WRITE_RECORD = "insert into %s values(0,0)";
    private static final String TEST_GP_UPDATE_RECORD = "update %s set col2=1 where 1=1";
    private static final String TEST_GP_DELETE_RECORD = "delete from %s where 1=1";
}
