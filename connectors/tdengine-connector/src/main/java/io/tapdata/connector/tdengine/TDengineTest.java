package io.tapdata.connector.tdengine;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

// TODO: 2022/6/9 need to improve test items 
public class TDengineTest extends CommonDbTest {

    protected static final String TEST_CREATE_TABLE = "create table %s(col1 timestamp, col2 int)";
    protected static final String TEST_WRITE_RECORD = "insert into %s values(now(), 0)";
    protected static final String TEST_DELETE_RECORD = "delete from %s";
    protected static final String TEST_DROP_TABLE = "drop table %s";
    protected static final String TEST_WRITE_SUCCESS = "Create,Insert,Delete,Drop succeed";

    public TDengineTest() {
        super();
    }

    public TDengineTest(TDengineConfig tdengineConfig, Consumer<TestItem> consumer) {
        super(tdengineConfig, consumer);
        // TDengine增量数据需要安装驱动，所以在连接测试处不检测
        testFunctionMap.remove("testStreamRead");
        jdbcContext = DataSourcePool.getJdbcContext(tdengineConfig, TDengineJdbcContext.class, uuid);
    }

    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("3.*");
    }

    @Override
    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_CREATE_TABLE, TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_WRITE_RECORD, TEST_WRITE_TABLE));
            //update
//            sqls.add(String.format(TEST_UPDATE_RECORD, TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_DELETE_RECORD, TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    @Override
    public void close() {
        try {
            jdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

}
