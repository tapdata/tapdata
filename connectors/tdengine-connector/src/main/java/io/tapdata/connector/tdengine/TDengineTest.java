package io.tapdata.connector.tdengine;

import com.google.common.collect.Lists;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.*;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

// TODO: 2022/6/9 need to improve test items 
public class TDengineTest extends CommonDbTest {

    protected static final String TEST_CREATE_TABLE = "create table %s(col1 timestamp, col2 int)";
    protected static final String TEST_WRITE_RECORD = "insert into %s values(now(), 0)";
    protected static final String TEST_DELETE_RECORD = "delete from %s";
    protected static final String TEST_DROP_TABLE = "drop table %s";
    protected static final String TEST_WRITE_SUCCESS = "Create,Insert,Delete,Drop succeed";

    protected static final String TEST_STREAM_READ_SUCCESS = "stream read succeed";

    private final static String DATABASE_READ_RIGHT = "SHOW CREATE DATABASE %s";

    public TDengineTest() {
        super();
    }

    public TDengineTest(TDengineConfig tdengineConfig, Consumer<TestItem> consumer) {
        super(tdengineConfig, consumer);
        jdbcContext = new TDengineJdbcContext(tdengineConfig);
    }

    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("3.*");
    }

    @Override
    public Boolean testReadPrivilege() {
        try {

            jdbcContext.query(String.format(DATABASE_READ_RIGHT,
                    commonDbConfig.getDatabase()), resultSet -> {
                if (Objects.isNull(resultSet) || resultSet.getMetaData().getColumnCount() < 1) {
                    consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                            "Current user may have no read privilege for database"));
                } else {
                    consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY, "All tables can be selected"));
                }
            });
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    @Override
    public Boolean testStreamRead() {
        Properties properties = new Properties();
//            properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6030");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, String.format("%s:%s", jdbcContext.getConfig().getHost(), 6030));
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, Boolean.TRUE.toString());
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, Boolean.TRUE.toString());
        properties.setProperty(TMQConstants.GROUP_ID, "test_group_id");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER,
                "io.tapdata.connector.tdengine.subscribe.TDengineResultDeserializer");
        try (TaosConsumer<Map<String, Object>> taosConsumer = new TaosConsumer<>(properties)) {
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY, TEST_STREAM_READ_SUCCESS));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "Tapdata server has no TDengine client"));
        }
        return false;
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

}
