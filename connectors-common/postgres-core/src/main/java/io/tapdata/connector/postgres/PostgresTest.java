package io.tapdata.connector.postgres;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.entity.TestItem;
import org.postgresql.Driver;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class PostgresTest extends CommonDbTest {

    public PostgresTest() {
        super();
    }

    public PostgresTest(PostgresConfig postgresConfig, Consumer<TestItem> consumer) {
        super(postgresConfig, consumer);
    }

    public PostgresTest initContext() {
        jdbcContext = new PostgresJdbcContext((PostgresConfig) commonDbConfig);
        return this;
    }

    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("9.4", "9.5", "9.6", "1*");
    }

    //Test number of tables and privileges
    public Boolean testReadPrivilege() {
        try {
            AtomicInteger tableSelectPrivileges = new AtomicInteger();
            jdbcContext.queryWithNext(String.format(PG_TABLE_SELECT_NUM, commonDbConfig.getUser(),
                    commonDbConfig.getDatabase(), commonDbConfig.getSchema()), resultSet -> tableSelectPrivileges.set(resultSet.getInt(1)));
            if (tableSelectPrivileges.get() >= tableCount()) {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY, "All tables can be selected"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no read privilege for some tables, Check it"));
            }
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    public Boolean testStreamRead() {
        Properties properties = new Properties();
        properties.put("user", commonDbConfig.getUser());
        properties.put("password", commonDbConfig.getPassword());
        properties.put("replication", "database");
        properties.put("assumeMinServerVersion", "9.4");
        try {
            Connection connection = new Driver().connect(commonDbConfig.getDatabaseUrl(), properties);
            assert connection != null;
            connection.close();
            List<String> testSqls = TapSimplify.list();
            String testSlotName = "test_tapdata_" + UUID.randomUUID().toString().replaceAll("-", "_");
            testSqls.add(String.format(PG_LOG_PLUGIN_CREATE_TEST, testSlotName, ((PostgresConfig) commonDbConfig).getLogPluginName()));
            testSqls.add(PG_LOG_PLUGIN_DROP_TEST);
            jdbcContext.batchExecute(testSqls);
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY, "Cdc can work normally"));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    String.format("Test log plugin failed: {%s}, Maybe cdc events cannot work", ErrorKit.getLastCause(e).getMessage())));
            return null;
        }
    }

    protected int tableCount() throws Throwable {
        AtomicInteger tableCount = new AtomicInteger();
        jdbcContext.queryWithNext(PG_TABLE_NUM, resultSet -> tableCount.set(resultSet.getInt(1)));
        return tableCount.get();
    }

    private final static String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";
    private final static String PG_TABLE_SELECT_NUM = "SELECT count(*) FROM information_schema.table_privileges " +
            "WHERE grantee='%s' AND table_catalog='%s' AND table_schema='%s' AND privilege_type='SELECT'";
    protected final static String PG_LOG_PLUGIN_CREATE_TEST = "SELECT pg_create_logical_replication_slot('%s','%s')";
    protected final static String PG_LOG_PLUGIN_DROP_TEST = "select pg_drop_replication_slot(a.slot_name) " +
            "from (select * from pg_replication_slots where slot_name like 'test_tapdata_%') a;";

    @Override
    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("\"" + commonDbConfig.getSchema() + "\".") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
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
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, e.getMessage()));
        }
        return true;
    }
}
