package io.tapdata.connector.doris;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import io.tapdata.connector.mysql.constant.MysqlTestItem;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.getStackString;
import static io.tapdata.base.ConnectorBase.testItem;

public class DorisConnectionTest extends CommonDbTest {
    public static final String TAG = DorisConnectionTest.class.getSimpleName();

    protected static final String CHECK_DATABASE_PRIVILEGES_SQL = "SHOW GRANTS";
    protected static final String CHECK_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
            "FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
            "WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'CREATE'";
    protected static String CHECK_TIDB_VERSION ="SELECT VERSION()";

    protected   DorisContext dorisContext;

    protected  TapConnectionContext tapConnectionContext;
    protected ConnectionOptions connectionOptions;
    protected  DorisConfig dorisConfig;
    public DorisConnectionTest(DorisContext dorisContext, TapConnectionContext tapConnectionContext,
                               Consumer<TestItem> consumer, CommonDbConfig commonDbConfig, ConnectionOptions connectionOptions) {
        super(commonDbConfig, consumer);
        this.dorisContext = dorisContext;
        this.tapConnectionContext = tapConnectionContext;
        this.dorisConfig=dorisContext.getDorisConfig();
        this.connectionOptions = connectionOptions;
        if (!ConnectionTypeEnum.SOURCE.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testWritePrivilege", this::testWritePrivilege);
            testFunctionMap.put("testCreateTablePrivilege", this::testCreateTablePrivilege);
        }
        if (!ConnectionTypeEnum.TARGET.getType().equals(commonDbConfig.get__connectionType())) {
//            testFunctionMap.put("testBinlogMode", this::testBinlogMode);
//            testFunctionMap.put("testBinlogRowImage", this::testBinlogRowImage);
//            testFunctionMap.put("testCDCPrivileges", this::testCDCPrivileges);
//            testFunctionMap.put("setCdcCapabilitie", this::setCdcCapabilitie);
        }
    }
    public Boolean testVersion() {
        AtomicReference<String> version = new AtomicReference<>();
        try {
           Statement statement= dorisContext.getConnection().createStatement();
           ResultSet resultSet = statement.executeQuery("select version() as version");
            while(resultSet.next()){
                String versionMsg =resultSet.getString("version");
                version.set(versionMsg);
            }

        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY,"version-"+version));
        return true;
    }
    @Override
    public Boolean testOneByOne() {
        testFunctionMap.put("testVersion", this::testVersion);
        testFunctionMap.put("testConnect",this::testConnect);
//        testFunctionMap.put("testDorisHttp",this::testDorisHttp);
        if (!ConnectionTypeEnum.SOURCE.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testCreateTablePrivilege", this::testCreateTablePrivilege);
        }
        if (!ConnectionTypeEnum.TARGET.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testReadPrivilege", this::testReadPrivilege);
        }
        return super.testOneByOne();
    }

    public Boolean testCreateTablePrivilege() {
        try {
            String username =dorisConfig.getUser();
            boolean missed = checkDorisCreateTablePrivilege(username);
            if (missed) {
                consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "User does not have privileges [ create ], will not be able to use the create table(s) feature"));
                return true;
            }
            consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (SQLException e) {
            int errorCode = e.getErrorCode();
            String sqlState = e.getSQLState();
            String message = e.getMessage();

            // 如果源库是关闭密码认证时，默认权限校验通过
            if (errorCode == 1290 && "HY000".equals(sqlState) && StringUtils.isNotBlank(message) && message.contains("--skip-grant-tables")) {
                consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
            } else {
                consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Check create table privileges failed; " + e.getErrorCode() + " " + e.getSQLState() + " " + e.getMessage() + "\n" + getStackString(e)));
            }
        } catch (Throwable e) {
            consumer.accept(testItem(MysqlTestItem.CHECK_CREATE_TABLE_PRIVILEGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Check create table privileges failed; " + e.getMessage() + "\n" + getStackString(e)));
            return true;
        }
        return true;
    }
    protected boolean checkDorisCreateTablePrivilege(String username) throws Throwable {
        String sql = String.format(CHECK_CREATE_TABLE_PRIVILEGES_SQL, username);
        AtomicBoolean result = new AtomicBoolean(true);
       Statement statement = dorisContext.getConnection().createStatement();
         ResultSet resultSet =statement.executeQuery(sql);
            while (resultSet.next()) {
                if (resultSet.getInt(1) > 0) {
                    result.set(false);
                }
            }
        return result.get();
    }
    @Override
    public Boolean testConnect() {
        try (
                Connection connection = dorisContext.getConnection();
        ) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Exception e) {
            if (e instanceof SQLException) {
                String errMsg = e.getMessage();
                if (errMsg.contains("using password")) {
                    String password = dorisConfig.getPassword();
                    if (StringUtils.isNotEmpty(password)) {
                        errMsg = "password or username is error ,please check";
                    } else {
                        errMsg = "password is empty,please enter password";
                    }
                    consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, errMsg));
                }
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }
    @Override
    public Boolean testWritePrivilege() {
        return WriteOrReadPrivilege("write");
    }

    private boolean WriteOrReadPrivilege(String mark) {

      String databaseName = dorisConfig.getDatabase();
//        String databaseName=tapConnectionContext.getConnectionConfig().getString("databaseName");
        List<String> tableList = new ArrayList<>();
        AtomicReference<Boolean> globalWrite = new AtomicReference<>();
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        String itemMark = TestItem.ITEM_READ;
        if ("write".equals(mark)) {
            itemMark = TestItem.ITEM_WRITE;
        }
        try {
            Statement statement= dorisContext.getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery(CHECK_DATABASE_PRIVILEGES_SQL);
            while(resultSet.next()){
                String Sql =resultSet.getString("GlobalPrivs");
                if (Sql.contains("Admin_priv")){
                    return  true;
                }else {
                    String grantSql = resultSet.getString("DatabasePrivs");
                    String finalItemMark = itemMark;
                    if (testWriteOrReadPrivilege(grantSql, tableList, databaseName, mark)) {
                        testItem.set(testItem(finalItemMark, TestItem.RESULT_SUCCESSFULLY));
                        globalWrite.set(true);
                        return true;
                    }
                    }
                }

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
        privilege = grantSql.contains(" Load_priv") && grantSql.contains("Alter_priv") && grantSql.contains("Drop_priv")&&grantSql.contains(databaseName)
                || grantSql.contains("All");
        if ("read".equals(mark)) {
            privilege = (grantSql.contains("Select_priv")||grantSql.contains("Read_only")) && grantSql.contains(databaseName)|| grantSql.contains("All");
        }
        if (privilege){
            return  true;
        }
        return false;
    }
    @Override
    public Boolean testHostPort() {
        DataMap connectionConfig = tapConnectionContext.getConnectionConfig();
        String host = String.valueOf(connectionConfig.get("host"));
        int port = ((Number) connectionConfig.get("port")).intValue();
        try {
            NetUtil.validateHostPortWithSocket(host, port);
            consumer.accept(testItem(MysqlTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (IOException e) {
            consumer.accept(testItem(MysqlTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, e.getMessage()));
            return false;

        }
    }

}
