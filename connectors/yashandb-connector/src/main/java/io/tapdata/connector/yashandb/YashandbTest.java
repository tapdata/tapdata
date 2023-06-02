package io.tapdata.connector.yashandb;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.yashandb.config.YashandbConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.connection.ConnectionCheckItem;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * Author:Skeet
 * Date: 2023/5/22
 **/
public class YashandbTest extends CommonDbTest {
    public YashandbTest(YashandbConfig yashandbConfig, Consumer<TestItem> consumer) {
        super(yashandbConfig, consumer);
    }

    public YashandbTest initContext() {
        jdbcContext = new YashandbJdbcContext(commonDbConfig);
        return this;
    }

    protected static final String TEST_WRITE_TABLE = "TAPDATA___TEST";
    @Override
    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("\"" + commonDbConfig.getSchema() + "\".") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE)).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_WRITE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_UPDATE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_DELETE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        } finally {
            EmptyKit.closeQuietly(jdbcContext);
        }
        return true;
    }
}
