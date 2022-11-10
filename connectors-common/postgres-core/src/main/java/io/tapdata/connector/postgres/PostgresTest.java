package io.tapdata.connector.postgres;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.constant.DbTestItem;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.entity.TestItem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class PostgresTest extends CommonDbTest {

    public PostgresTest() {
        super();
    }

    public PostgresTest(PostgresConfig postgresConfig, Consumer<TestItem> consumer) {
        super(postgresConfig, consumer);
        jdbcContext = DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class, uuid);
    }

    @Override
    public Boolean testOneByOne() {
        testFunctionMap.put("testPrivilege", this::testPrivilege);
        testFunctionMap.put("testReplication", this::testReplication);
        testFunctionMap.put("testLogPlugin", this::testLogPlugin);
        return super.testOneByOne();
    }

    //Test number of tables and privileges
    public Boolean testPrivilege() {
        try (
                Connection conn = jdbcContext.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT count(*) FROM information_schema.table_privileges " +
                                "WHERE grantee=? AND table_catalog=? AND table_schema=? ")
        ) {
            AtomicInteger tablePrivileges = new AtomicInteger();
            ps.setObject(1, commonDbConfig.getUser());
            ps.setObject(2, commonDbConfig.getDatabase());
            ps.setObject(3, commonDbConfig.getSchema());
            jdbcContext.query(ps, resultSet -> tablePrivileges.set(resultSet.getInt(1)));
            if (tablePrivileges.get() >= 7 * tableCount()) {
                consumer.accept(testItem(DbTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
            } else {
                consumer.accept(testItem(DbTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no all privileges for some tables, Check it!"));
            }
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(DbTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    public Boolean testReplication() {
        try {
            AtomicBoolean rolReplication = new AtomicBoolean();
            jdbcContext.queryWithNext(String.format(PG_ROLE_INFO, commonDbConfig.getUser()),
                    resultSet -> rolReplication.set(resultSet.getBoolean("rolreplication")));
            if (rolReplication.get()) {
                consumer.accept(testItem(DbTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY));
            } else {
                consumer.accept(testItem(DbTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user have no privileges to create Replication Slot!"));
            }
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(DbTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    public Boolean testLogPlugin() {
        try {
            List<String> testSqls = TapSimplify.list();
            String testSlotName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");
            testSqls.add(String.format(PG_LOG_PLUGIN_CREATE_TEST, testSlotName, ((PostgresConfig) commonDbConfig).getLogPluginName()));
            testSqls.add(String.format(PG_LOG_PLUGIN_DROP_TEST, testSlotName));
            jdbcContext.batchExecute(testSqls);
            consumer.accept(testItem(DbTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(DbTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    String.format("Test log plugin failed: {%s}, Maybe cdc events cannot work!", e.getMessage())));
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

    private final static String PG_ROLE_INFO = "SELECT * FROM pg_roles WHERE rolname='%s'";
    private final static String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";
    private final static String PG_LOG_PLUGIN_CREATE_TEST = "SELECT pg_create_logical_replication_slot('%s','%s')";
    private final static String PG_LOG_PLUGIN_DROP_TEST = "SELECT pg_drop_replication_slot('%s')";
}
