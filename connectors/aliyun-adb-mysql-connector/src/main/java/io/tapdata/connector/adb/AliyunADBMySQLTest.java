package io.tapdata.connector.adb;

import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.List;
import java.util.function.Consumer;

public class AliyunADBMySQLTest extends MysqlConnectionTest {

    public AliyunADBMySQLTest(MysqlConfig mysqlConfig, Consumer<TestItem> consumer) {
        super(mysqlConfig, consumer);
        testFunctionMap.remove("testBinlogMode");
        testFunctionMap.remove("testBinlogRowImage");
        testFunctionMap.remove("testCDCPrivileges");
    }

    protected String getGrantsSql() {
        return "SHOW GRANTS";
    }

    public boolean testWriteOrReadPrivilege(String grantSql, List<String> tableList, String databaseName, String mark) {
        boolean privilege;
        privilege = grantSql.contains("INSERT") && grantSql.contains("UPDATE") && grantSql.contains("DELETE")
                || grantSql.contains("ALL ");
        if ("read".equals(mark)) {
            privilege = grantSql.contains("SELECT") || grantSql.contains("ALL ");
        }
        grantSql = grantSql.replaceAll("\\\\", "");
        if (grantSql.contains("`*`.`*` TO")) {
            return privilege;
        } else if (grantSql.contains("`" + databaseName + "`.`*` TO") || grantSql.contains(databaseName + ".`*` TO")) {
            return privilege;
        } else if (databaseName.contains("_") &&
                (grantSql.contains("`" + databaseName.replace("_", "\\_") + "`.`*` TO") ||
                        grantSql.contains(databaseName.replace("_", "\\_") + ".`*` TO"))) {
            return privilege;
        } else if (grantSql.contains(databaseName + ".")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName + "."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        } else if (grantSql.contains("`" + databaseName + "`.")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName + "`."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        } else if (databaseName.contains("_") && grantSql.contains(databaseName.replace("_", "\\_") + ".")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName.replace("_", "\\_") + "."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        } else if (databaseName.contains("_") && grantSql.contains("`" + databaseName.replace("_", "\\_") + "`.")) {
            String table = grantSql.substring(grantSql.indexOf(databaseName.replace("_", "\\_") + "`."), grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        }
        return false;
    }

}
