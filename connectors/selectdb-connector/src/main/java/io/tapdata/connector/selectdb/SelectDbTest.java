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

    @Override
    public Boolean testWritePrivilege() {
        return WriteOrReadPrivilege("write");
    }

    private boolean WriteOrReadPrivilege(String mark) {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String databaseName = String.valueOf(connectionConfig.get("database"));
        List<String> tableList = new ArrayList();
        AtomicReference<Boolean> globalWrite = new AtomicReference();
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        String itemMark = TestItem.ITEM_READ;
        if ("write".equals(mark)) {
            itemMark = TestItem.ITEM_WRITE;
        }
        try {
            String finalItemMark = itemMark;
            selectDbJdbcContext.query(CHECK_DATABASE_PRIVILEGES_SQL, resultSet -> {
                while (resultSet.next()) {
                    String grantSql = resultSet.getString(1);
                    if (testWriteOrReadPrivilege(grantSql, tableList, databaseName, mark)) {
                        testItem.set(testItem(finalItemMark, TestItem.RESULT_SUCCESSFULLY));
                        globalWrite.set(true);
                        return;
                    }
                }

            });
        } catch (Throwable e) {
            consumer.accept(testItem(itemMark, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
        if (globalWrite.get() != null) {
            consumer.accept(testItem.get());
            return true;
        }
        if (CollectionUtils.isNotEmpty(tableList)) {
            consumer.accept(testItem(itemMark, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, JSONObject.toJSONString(tableList)));
            return true;
        }
        consumer.accept(testItem(itemMark, TestItem.RESULT_FAILED, "Without table can " + mark));
        return false;
    }

    public boolean testWriteOrReadPrivilege(String grantSql, List<String> tableList, String databaseName, String mark) {
        boolean privilege;
        privilege = grantSql.contains("INSERT") && grantSql.contains("UPDATE") && grantSql.contains("DELETE")
                || grantSql.contains("ALL PRIVILEGES");
        if ("read".equals(mark)) {
            privilege = grantSql.contains("SELECT") || grantSql.contains("ALL PRIVILEGES");
        }
        if (grantSql.contains("*.* TO")) {
            if (privilege) {
                return true;
            }

        } else if (grantSql.contains("`" + databaseName + "`" + ".* TO")) {
            if (privilege) {
                return true;
            }
        } else if (grantSql.contains("`" + databaseName + "`" + ".")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName + "."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        }
        return false;
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
