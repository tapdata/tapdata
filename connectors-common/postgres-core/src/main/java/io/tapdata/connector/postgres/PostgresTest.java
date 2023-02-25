package io.tapdata.connector.postgres;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.constant.DbTestItem;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.List;
import java.util.UUID;
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
        jdbcContext = DataSourcePool.getJdbcContext(commonDbConfig, PostgresJdbcContext.class, uuid);
        return this;
    }

    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("9.4", "9.5", "9.6", "10.*", "11.*", "12.*");
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
        try {
            List<String> testSqls = TapSimplify.list();
            String testSlotName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
            testSqls.add(String.format(PG_LOG_PLUGIN_CREATE_TEST, testSlotName, ((PostgresConfig) commonDbConfig).getLogPluginName()));
            testSqls.add(String.format(PG_LOG_PLUGIN_DROP_TEST, testSlotName));
            jdbcContext.batchExecute(testSqls);
            consumer.accept(testItem(DbTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY, "Cdc can work normally"));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(DbTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    String.format("Test log plugin failed: {%s}, Maybe cdc events cannot work", e.getCause().getMessage())));
            return null;
        }
    }

    protected int tableCount() throws Throwable {
        AtomicInteger tableCount = new AtomicInteger();
        jdbcContext.queryWithNext(PG_TABLE_NUM, resultSet -> tableCount.set(resultSet.getInt(1)));
        return tableCount.get();
    }

    @Override
    public void close() {
        try {
            jdbcContext.finish(uuid);
        } catch (Exception ignored) {
        }
    }

    private final static String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";
    private final static String PG_TABLE_SELECT_NUM = "SELECT count(*) FROM information_schema.table_privileges " +
            "WHERE grantee='%s' AND table_catalog='%s' AND table_schema='%s' AND privilege_type='SELECT'";
    private final static String PG_LOG_PLUGIN_CREATE_TEST = "SELECT pg_create_logical_replication_slot('%s','%s')";
    private final static String PG_LOG_PLUGIN_DROP_TEST = "SELECT pg_drop_replication_slot('%s')";
}
