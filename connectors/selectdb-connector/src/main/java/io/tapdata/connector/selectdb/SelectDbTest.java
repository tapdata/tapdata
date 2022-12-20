package io.tapdata.connector.selectdb;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.selectdb.config.SelectDbConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * Author:Skeet
 * Date: 2022/12/9
 **/

public class SelectDbTest extends CommonDbTest {
    protected static final String TAG = SelectDbTest.class.getSimpleName();

    protected static final String CHECK_DATABASE_PRIVILEGES_SQL = "SHOW GRANTS FOR CURRENT_USER";

    protected TapConnectionContext tapConnectionContext;
    protected SelectDbJdbcContext selectDbJdbcContext;


    public SelectDbTest(SelectDbConfig selectDbConfig, Consumer<TestItem> consumer) {
        super(selectDbConfig, consumer);
    }

    public SelectDbTest initContext() {
        jdbcContext = DataSourcePool.getJdbcContext(commonDbConfig, SelectDbJdbcContext.class, uuid);
        return this;
    }

    protected static final String TEST_CREATE_TABLE = "create table %s(`user_id` VARCHAR(255) COMMENT 'ti',\n" +
            "`username` VARCHAR(255) COMMENT 'name')\n" +
            "COMMENT \"测试数据2\"\n" +
            "DISTRIBUTED BY HASH(`user_id`)";
    protected static final String TEST_WRITE_RECORD = "insert into %s (user_id, username) VALUES ('1', 'test1')";
//    protected static final String TEST_UPDATE_RECORD = "update %s set username='test2' where user_id = '1'";
    protected static final String TEST_DELETE_RECORD = "delete from %s where user_id = 1;";
    protected static final String TEST_DROP_TABLE = "drop table %s";
    protected static final String TEST_WRITE_SUCCESS = "Create,Insert,Update,Delete,Drop succeed";

    @Override
    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
//            selectDbJdbcContext =(SelectDbJdbcContext) DataSourcePool.getJdbcContext(commonDbConfig, SelectDbJdbcContext.class, uuid);
//            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
//                sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
//            }
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
    protected List<String> supportVersions() {
        return Lists.newArrayList("5.7", "8.0");
    }

    @Override
    public void close() {
        try {
            jdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

}
