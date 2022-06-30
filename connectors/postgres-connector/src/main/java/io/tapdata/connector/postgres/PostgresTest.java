package io.tapdata.connector.postgres;

import io.tapdata.common.CommonDbTest;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.entity.TestItem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.base.ConnectorBase.testItem;

// TODO: 2022/6/9 need to improve test items 
public class PostgresTest extends CommonDbTest {

    public PostgresTest(PostgresConfig postgresConfig) {
        super(postgresConfig);
        jdbcContext = DataSourcePool.getJdbcContext(postgresConfig, PostgresJdbcContext.class, uuid);
    }

    //Test number of tables and privileges
    public TestItem testPrivilege() {
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
                return testItem(DbTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(DbTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no all privileges for some tables, Check it!");
            }
        } catch (Throwable e) {
            return testItem(DbTestItem.CHECK_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testReplication() {
        try {
            AtomicBoolean rolReplication = new AtomicBoolean();
            jdbcContext.query(String.format(PG_ROLE_INFO, commonDbConfig.getUser()),
                    resultSet -> rolReplication.set(resultSet.getBoolean("rolreplication")));
            if (rolReplication.get()) {
                return testItem(DbTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY);
            } else {
                return testItem(DbTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user have no privileges to create Replication Slot!");
            }
        } catch (Throwable e) {
            return testItem(DbTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testLogPlugin() {
        try {
            List<String> testSqls = TapSimplify.list();
            testSqls.add(String.format(PG_LOG_PLUGIN_CREATE_TEST, ((PostgresConfig) commonDbConfig).getLogPluginName()));
            testSqls.add(PG_LOG_PLUGIN_DROP_TEST);
            jdbcContext.batchExecute(testSqls);
            return testItem(DbTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY);
        } catch (Throwable e) {
            return testItem(DbTestItem.CHECK_LOG_PLUGIN.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Invalid log plugin, Maybe cdc events cannot work!");
        }
    }

    private int tableCount() throws Throwable {
        AtomicInteger tableCount = new AtomicInteger();
        jdbcContext.query(PG_TABLE_NUM, resultSet -> tableCount.set(resultSet.getInt(1)));
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
    private final static String PG_LOG_PLUGIN_CREATE_TEST = "SELECT pg_create_logical_replication_slot('pg_slot_test','%s')";
    private final static String PG_LOG_PLUGIN_DROP_TEST = "SELECT pg_drop_replication_slot('pg_slot_test')";
}
