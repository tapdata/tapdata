package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.schema.TapField;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.list;

@TapGo(tag = "V2", sort = 70)
@DisplayName("getTableNames.test")//GetTableNamesFunction获得表名列表
public class GetTableNamesFunctionTest extends PDKTestBase {
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("getTableNames.test.wait"));
        }
    }
    @DisplayName("getTableNames.discover")//用例1， 发现表
    @Test
    @TapTestCase(sort = 1)
    /**
     * 指定GetTableNamesFunction方法之后， 至少返回一张表， 即为成功
     * */
    void discover() {
        System.out.println(LangUtil.format("getTableNames.discover.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discover");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
                GetTableNamesFunction tableNamesFun = connectorFunctions.getGetTableNamesFunction();

                //指定GetTableNamesFunction方法之后， 至少返回一张表， 即为成功
                tableNamesFun.tableNames(
                        connectorContext, 10,
                        consumer -> {
                            //执行discoverSchema之后， 至少返回一张表
                            TapAssert.asserts(() -> {
                                Assertions.assertTrue(
                                        null != consumer && !consumer.isEmpty(),
                                        LangUtil.format("getTableNames.discover.notAnyTable"));
                            }).acceptAsError(
                                    testCase,
                                    LangUtil.format("getTableNames.discover.succeed", consumer.size())
                            );
                        }
                );
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    //TODO
    @DisplayName("getTableNames.afterCreate")//用例2， 建表之后能发现表（依赖CreateTableFunction）
    @Test
    @TapTestCase(sort = 2)
    /**
     * 通过CreateTableFunction创建一张表， 表名随机，
     * 建表之后执行GetTableNamesFunction方法获得表列表，
     * 表列表里包含随机创建的表即为成功。
     * 验证结束之后需要删掉随机建的表（依赖DropTableFunction）
     * */
    void discoverAfterCreateTable() {
        System.out.println(LangUtil.format("getTableNames.discoverAfterCreateTable.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discoverAfterCreateTable");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                connector.discoverSchema(connectorContext, new ArrayList<>(), 1000, consumer -> {
                    //执行discoverSchema之后， 至少返回一张表
                    TapAssert.asserts(() -> {
                        Assertions.assertTrue(
                                null != consumer && !consumer.isEmpty(),
                                LangUtil.format("discover.notAnyTable"));
                    }).acceptAsError(
                            testCase,
                            LangUtil.format("discover.succeed", consumer.size())
                    );

                    Map<String, Map<String, String>> warnFieldMap = new HashMap<>();
                    consumer.stream().forEach(table -> {
                        if (null == table) {
                            TapAssert.asserts(() ->
                                    Assertions.fail(LangUtil.format("discover.nullTable"))
                            ).error(testCase);
                            return;
                        }
                        //表里有表名即为成功
                        String tableName = table.getId();
                        if (null == tableName || "".equals(tableName)) {
                            TapAssert.asserts(() ->
                                    Assertions.fail(LangUtil.format("discover.emptyTableName"))
                            ).error(testCase);
                            return;
                        }
                        //表里没有字段描述时，报警告
                        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
                        TapAssert.asserts(() ->
                                Assertions.assertTrue(
                                        null != nameFieldMap && !nameFieldMap.isEmpty(),
                                        LangUtil.format("discover.emptyTFields", tableName))
                        ).warn(testCase);

                        //表里有字段， 但是字段的name或者dataType为空时， 报警告， 具体哪些字段有问题
                        if (null != nameFieldMap && !nameFieldMap.isEmpty()) {
                            for (Map.Entry<String, TapField> field : nameFieldMap.entrySet()) {
                                TapField value = field.getValue();
                                String name = value.getName();
                                String type = value.getDataType();
                                if (null == name || "".equals(name) || null == type || "".equals(type)) {
                                    Map<String, String> stringStringMap = warnFieldMap.computeIfAbsent(tableName, tables -> null == warnFieldMap.get(tables) ? new HashMap<>() : warnFieldMap.get(tables));
                                    stringStringMap.put(name, type);
                                }
                            }
                        }
                    });
                    StringBuilder warn = new StringBuilder();
                    final String starLine = "\n\t\t\t\t\t";
                    warnFieldMap.forEach((table, value) -> {
                        warn.append(starLine).append(table).append(": ");
                        value.forEach((name, type) -> warn.append(starLine).append("\t").append(name).append(":\t").append(type));
                    });
                    TapAssert.asserts(() ->
                            Assertions.assertTrue(
                                    warnFieldMap.isEmpty(),
                                    LangUtil.format("discover.hasWarnFields", warn.toString()))
                    ).acceptAsWarn(testCase, LangUtil.format("discover.notWarnFields"));
                });
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    //TODO
    @DisplayName("getTableNames.byCount")//用例3， 通过指定表数量加载固定数量的表（依赖已经存在多表）
    @Test
    @TapTestCase(sort = 3)
    /**
     * 执行discoverSchema之后， 发现有大于1张表的返回，
     * 通过int batchSize参数指定为1，
     * 通过Consumer<List<String>> consumer返回了一张表为成功， 否则按警告处理，
     * 如果只有一张表， 直接通过此测试。
     * */
    void discoverByTableCount() {
        System.out.println(LangUtil.format("getTableNames.discoverByTableCount.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discoverByTableCount");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                connector.discoverSchema(connectorContext, new ArrayList<>(), 1000, consumer -> {
                    //执行discoverSchema之后， 至少返回一张表
                    TapAssert.asserts(() -> {
                        Assertions.assertTrue(
                                null != consumer && !consumer.isEmpty(),
                                LangUtil.format("discover.notAnyTable"));
                    }).acceptAsError(
                            testCase,
                            LangUtil.format("discover.succeed", consumer.size())
                    );

                    Map<String, Map<String, String>> warnFieldMap = new HashMap<>();
                    consumer.stream().forEach(table -> {
                        if (null == table) {
                            TapAssert.asserts(() ->
                                    Assertions.fail(LangUtil.format("discover.nullTable"))
                            ).error(testCase);
                            return;
                        }
                        //表里有表名即为成功
                        String tableName = table.getId();
                        if (null == tableName || "".equals(tableName)) {
                            TapAssert.asserts(() ->
                                    Assertions.fail(LangUtil.format("discover.emptyTableName"))
                            ).error(testCase);
                            return;
                        }
                        //表里没有字段描述时，报警告
                        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
                        TapAssert.asserts(() ->
                                Assertions.assertTrue(
                                        null != nameFieldMap && !nameFieldMap.isEmpty(),
                                        LangUtil.format("discover.emptyTFields", tableName))
                        ).warn(testCase);

                        //表里有字段， 但是字段的name或者dataType为空时， 报警告， 具体哪些字段有问题
                        if (null != nameFieldMap && !nameFieldMap.isEmpty()) {
                            for (Map.Entry<String, TapField> field : nameFieldMap.entrySet()) {
                                TapField value = field.getValue();
                                String name = value.getName();
                                String type = value.getDataType();
                                if (null == name || "".equals(name) || null == type || "".equals(type)) {
                                    Map<String, String> stringStringMap = warnFieldMap.computeIfAbsent(tableName, tables -> null == warnFieldMap.get(tables) ? new HashMap<>() : warnFieldMap.get(tables));
                                    stringStringMap.put(name, type);
                                }
                            }
                        }
                    });
                    StringBuilder warn = new StringBuilder();
                    final String starLine = "\n\t\t\t\t\t";
                    warnFieldMap.forEach((table, value) -> {
                        warn.append(starLine).append(table).append(": ");
                        value.forEach((name, type) -> warn.append(starLine).append("\t").append(name).append(":\t").append(type));
                    });
                    TapAssert.asserts(() ->
                            Assertions.assertTrue(
                                    warnFieldMap.isEmpty(),
                                    LangUtil.format("discover.hasWarnFields", warn.toString()))
                    ).acceptAsWarn(testCase, LangUtil.format("discover.notWarnFields"));
                });
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(GetTableNamesFunction.class, LangUtil.format(inNeedFunFormat, "GetTableNamesFunction"))
        );
    }
}
