package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.base.TddConfigKey;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.*;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("discoverSchema.test")//discoverSchema发现表， 必测方法
@TapGo(tag = "V2", sort = 9999, goTest = true, subTest = {DiscoverSchemaTestV2.class})//6
public class DiscoverSchemaTest extends PDKTestBase {
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("discoverSchema.test.wait"));
        }
    }
    @DisplayName("discoverSchema.discover")//用例1， 发现表
    @Test
    @TapTestCase(sort = 1)
    /**
     * 执行discoverSchema之后， 至少返回一张表， 表里有表名即为成功
     * 表里没有字段描述时， 报警告
     * 表里有字段， 但是字段的name或者dataType为空时， 报警告， 具体哪些字段有问题
     * */
    void discover() {
        System.out.println(LangUtil.format("discoverSchema.discover.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discover");
                prepare.recordEventExecute().testCase(testCase);
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                List<TapTable> tables = new ArrayList<>();
                connector.discoverSchema(connectorContext, new ArrayList<>(), 1000, consumer -> {
                    if (null != consumer) tables.addAll(consumer);
                });
                //执行discoverSchema之后， 至少返回一张表
                TapAssert.asserts(() ->
                        Assertions.assertFalse(tables.isEmpty(), LangUtil.format("discover.notAnyTable"))
                ).acceptAsWarn(
                        testCase,
                        LangUtil.format("discover.succeed", tables.size())
                );
                Map<String, Map<String, String>> warnFieldMap = new HashMap<>();
                tables.stream().forEach(table -> {
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
                                Map<String, String> stringStringMap = warnFieldMap.computeIfAbsent(tableName, ts -> null == warnFieldMap.get(ts) ? new HashMap<>() : warnFieldMap.get(ts));
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
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    @DisplayName("discoverSchema.discoverAfterCreate")//用例2， 建表之后能发现表（依赖CreateTableFunction）
    @Test
    @TapTestCase(sort = 2, dump = true)
    /**
     * 通过CreateTableFunction创建一张表， 表名随机，
     * 表里的字段属性是通过TapType的全类型11个字段推演得来，
     * 建表之后执行discoverySchema获得表列表，
     * 表列表里包含随机创建的表，
     * 且所有字段的name和dataType一致即为成功。
     * 验证结束之后需要删掉随机建的表（依赖DropTableFunction）
     * */
    void discoverAfterCreate() {
        System.out.println(LangUtil.format("discoverSchema.discoverAfterCreate.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discoverAfterCreate");
                execute.testCase(testCase);
                //通过CreateTableFunction创建一张表， 表名随机，
                //表里的字段属性是通过TapType的全类型11个字段推演得来，
                if (!(hasCreateTable = this.createTable(prepare, false))) return;
                String tableIdTarget = targetTable.getId();
                //建表之后执行discoverySchema获得表列表，
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
                //List<TapTable> consumer = new ArrayList<>();
                Map<String, TapTable> consumer = new HashMap<>();
                long discoverStart = System.currentTimeMillis();
                connector.discoverSchema(connectorContext, list(tableIdTarget), 100, con -> {
                    if (null != con)
                        consumer.putAll(con.stream().filter(Objects::nonNull).collect(Collectors.toMap(TapTable::getId, c -> c, (c1, c2) -> c1)));
                });
                String tableId = targetTable.getId();
                TapTable consumerTable = consumer.get(tableId);
                long discoverEnd = System.currentTimeMillis();
                boolean hasTargetTable = !consumer.isEmpty() && null != consumerTable;
                //表列表里包含随机创建的表，
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                hasTargetTable,
                                LangUtil.format("discoverAfterCreate.notFindTargetTable", tableId, discoverEnd - discoverStart))
                ).acceptAsError(
                        testCase,
                        LangUtil.format("discoverAfterCreate.fundTargetTable", tableId, discoverEnd - discoverStart)
                );
                if (hasTargetTable) {
                    //且所有字段的name和dataType一致即为成功。
                    LinkedHashMap<String, TapField> tapTableFieldMap = consumerTable.getNameFieldMap();
                    LinkedHashMap<String, TapField> targetTableFieldMap = super.modelDeduction(connectorNode);//targetTable.getNameFieldMap();
                    if (null == tapTableFieldMap || null == targetTableFieldMap) {
                        TapAssert.asserts(() -> Assertions.fail(LangUtil.format("discoverAfterCreate.exitsNullFiledMap", tableId))).error(testCase);
                        return;
                    }
                    int tapTableSize = tapTableFieldMap.size();
                    int targetTableSize = targetTableFieldMap.size();
                    try {
                        TapAssert.asserts(() -> {
                            Assertions.assertTrue(
                                    tapTableSize >= targetTableSize,
                                    LangUtil.format("discoverAfterCreate.fieldsNotEqualsCount",
                                            tapTableSize,
                                            targetTableSize
                                    )
                            );
                        }).acceptAsWarn(
                                testCase,
                                LangUtil.format(
                                        "discoverAfterCreate.fieldsEqualsCount",
                                        tapTableSize,
                                        targetTableSize
                                )
                        );
                    } catch (Exception ignored) {
                    }

                    boolean hasSuchField = true;
                    Iterator<Map.Entry<String, TapField>> iterator = targetTableFieldMap.entrySet().stream().iterator();
                    String targetFieldItem = "";
                    String tapFieldItem = "";
                    while (iterator.hasNext()) {
                        Map.Entry<String, TapField> next = iterator.next();
                        TapField field = next.getValue();
                        String name = field.getName();
                        String dataType = field.getDataType();

                        TapField tapField = tapTableFieldMap.get(name);
                        if (null == tapField) {
                            hasSuchField = false;
                            targetFieldItem = "(" + name + ":" + dataType + ")";
                            tapFieldItem = "null";
                            break;
                        }
                        String tapDataType = tapField.getDataType();
                        if (null == dataType && tapDataType == null) {
                            continue;
                        }
                        if ((null == dataType && tapDataType != null) || !dataType.equals(tapDataType)) {
                            hasSuchField = false;
                            targetFieldItem = "(" + name + ":" + dataType + ")";
                            tapFieldItem = "(" + name + ":" + tapDataType + ")";
                            break;
                        }
                    }
                    final boolean hasSuchFieldFinal = hasSuchField;
                    final String tapFieldItemFinal = tapFieldItem;
                    final String targetFieldItemFinal = targetFieldItem;
                    TapAssert.asserts(() -> {
                        Assertions.assertTrue(
                                hasSuchFieldFinal,
                                LangUtil.format("discoverAfterCreate.allFieldNotEquals", tableId, targetFieldItemFinal, tapFieldItemFinal));
                    }).acceptAsWarn(
                            testCase,
                            LangUtil.format("discoverAfterCreate.allFieldEquals", tableId)
                    );
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                //验证结束之后需要删掉随机建的表（依赖DropTableFunction）
                if (hasCreateTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    @DisplayName("discoverSchema.discoverByTableName1")//用例3， 通过指定表明加载特定表（依赖已经存在多表）
    @Test
    @TapTestCase(sort = 3)
    /**
     * 执行discoverSchema之后，
     * 发现有大于1张表的返回，
     * 通过指定第一张表之后的任意一张表名，
     * 通过List<String> tables参数指定那张表，
     * 通过Consumer<List<TapTable>> consumer返回了这一张且仅此一张表为成功。
     * 如果只有一张表， 直接通过此测试。
     * */
    void discoverByTableName1() {
        System.out.println(LangUtil.format("discoverSchema.discoverByTableName1.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discoverByTableName1");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();

                AtomicInteger tableCount = new AtomicInteger();
                //通过指定第一张表之后的任意一张表名，
                AtomicInteger nextTable = new AtomicInteger();
                AtomicReference<TapTable> tapTableAto = new AtomicReference<>();

                long discoverStart = System.currentTimeMillis();
                List<TapTable> consumer = new ArrayList<>();
                connector.discoverSchema(connectorContext, new ArrayList<>(), 1000, consumer::addAll);

                long discoverEndTemp = System.currentTimeMillis();
                //执行discoverSchema之后， 至少返回一张表
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(
                            !consumer.isEmpty() && consumer.size() > 1,
                            LangUtil.format("discoverByTableName1.notAnyTable", discoverEndTemp - discoverStart));
                }).acceptAsWarn(
                        testCase,
                        LangUtil.format("discoverByTableName1.succeed", consumer.size(), discoverEndTemp - discoverStart)
                );
                if (!consumer.isEmpty() && consumer.size() > 1) {
                    tableCount.set(consumer.size());
                    //通过指定第一张表之后的任意一张表名，
                    nextTable.set(((new Random()).nextInt(tableCount.get() - 1) + 1));
                    tapTableAto.set(consumer.get(nextTable.get()));


                    //通过List<String> tables参数指定那张表，
                    TapTable tapTable = tapTableAto.get();
                    List<String> tables = list(tapTable.getId());
                    try {
                        //通过Consumer<List<TapTable>> consumer返回了这一张且仅此一张表为成功。
                        long discoverStart2 = System.currentTimeMillis();
                        connector.discoverSchema(connectorContext, tables, 1000, c -> {
                            long discoverEnd = System.currentTimeMillis();
                            TapAssert.asserts(() -> {
                                Assertions.assertTrue(
                                        null != c && c.size() == 1,
                                        LangUtil.format("discoverByTableName1.notAnyTableAfter", tableCount, tapTable.getId(), discoverEnd - discoverStart2));
                            }).acceptAsWarn(
                                    testCase,
                                    LangUtil.format("discoverByTableName1.succeedAfter", tableCount, tapTable.getId(), c.size(), discoverEnd - discoverStart2)
                            );
                        });
                    } catch (Throwable e) {
                    }
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });

    }

    @DisplayName("discoverSchema.discoverByTableName2")//用例4， 通过指定表名加载特定表（依赖CreateTableFunction）
    @Test
    @TapTestCase(sort = 4, dump = true)
    /**
     * 通过CreateTableFunction另外创建一张表，
     * 通过List<String> tables参数指定新创建的那张表，
     * 通过Consumer<List<TapTable>> consumer返回了这一张且仅此一张表为成功。
     * 验证结束之后需要删掉随机建的表（依赖DropTableFunction）
     * */
    void discoverByTableName2() {
        System.out.println(LangUtil.format("discoverSchema.discoverByTableName2.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreatedTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discoverByTableName2");
                execute.testCase(testCase);

                //通过CreateTableFunction另外创建一张表，
                Boolean deleteRecordAfterCreateTable = prepare.recordEventExecute().findTddConfig(TddConfigKey.DELETE_RECORD_AFTER_CREATE_TABLE.KeyName(), Boolean.class);
                if (!(hasCreatedTable = this.createTable(prepare, Optional.ofNullable(deleteRecordAfterCreateTable).orElse((Boolean) TddConfigKey.DELETE_RECORD_AFTER_CREATE_TABLE.defaultValue())))) {
                    return;
                }
                String tableIdTarget = targetTable.getId();

                //通过List<String> tables参数指定新创建的那张表，
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                Map<String, TapTable> tabMap = new HashMap<>();
                //通过Consumer<List<TapTable>> consumer返回了这一张且仅此一张表为成功。
                long discoverStart = System.currentTimeMillis();
                connector.discoverSchema(connectorContext, list(tableIdTarget), 1000, con -> {
                    if (null != con) {
                        tabMap.putAll(con.stream()
                                .filter(Objects::nonNull)
                                .collect(Collectors.toMap(TapTable::getId, tab -> tab, (t1, t2) -> t2)));
                    }
                });
                long discoverEnd = System.currentTimeMillis();
                TapAssert.asserts(() -> {
                    Assertions.assertFalse(tabMap.isEmpty(), LangUtil.format("discoverByTableName2.notAnyTable", tableIdTarget, tableIdTarget, tabMap.size(), discoverEnd - discoverStart));
                }).acceptAsError(
                        testCase,
                        LangUtil.format("discoverByTableName2.succeed", tableIdTarget, tableIdTarget, tabMap.size(), discoverEnd - discoverStart)
                );
                TapTable tapTable = tabMap.get(tableIdTarget);
                TapAssert.asserts(() -> {
                    Assertions.assertNotNull(tapTable, LangUtil.format("discoverByTableName2.notEqualsTable", tableIdTarget, tableIdTarget, tapTable.getId(), discoverEnd - discoverStart));
                }).acceptAsError(
                        testCase,
                        LangUtil.format("discoverByTableName2.equalsTable", tableIdTarget, tableIdTarget, tapTable.getId(), discoverEnd - discoverStart)
                );
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                //验证结束之后需要删掉随机建的表（依赖DropTableFunction）
                if (hasCreatedTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });

    }

    @DisplayName("discoverSchema.discoverByTableCount1")//用例5， 通过指定表数量加载固定数量的表（依赖已经存在多表）
    @Test
    @TapTestCase(sort = 5)
    /**
     * 执行discoverSchema之后，
     * 发现有大于1张表的返回，
     * 通过int tableSize参数指定为1，
     * 通过Consumer<List<TapTable>> consumer返回了一张表为成功。
     * 如果只有一张表， 直接通过此测试。
     * */
    void discoverByTableCount1() {
        System.out.println(LangUtil.format("discoverSchema.discoverByTableCount1.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discoverByTableCount1");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                List<TapTable> consumer = new ArrayList<>();
                long discoverStart = System.currentTimeMillis();
                connector.discoverSchema(connectorContext, new ArrayList<>(), 1000, con -> {
                    if (con != null) consumer.addAll(con);
                });
                long discoverEnd = System.currentTimeMillis();
                //执行discoverSchema之后， 发现有大于1张表的返回，
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(
                            !consumer.isEmpty() && consumer.size() > 1,
                            LangUtil.format("discoverByTableCount1.notAnyTable", discoverEnd - discoverStart));
                }).acceptAsWarn(
                        testCase,
                        LangUtil.format("discoverByTableCount1.succeed", consumer.size(), discoverEnd - discoverStart)
                );

                //通过int tableSize参数指定为1，
                final int tableCount = 100;
                //通过Consumer<List<TapTable>> consumer返回了一张表为成功。
                List<List<TapTable>> consumer2 = new ArrayList<>();
                long discoverStart2 = System.currentTimeMillis();
                // @TODO tableSize = tableCount
                connector.discoverSchema(connectorContext, list(), tableCount, con -> {
                    if (null != con) consumer2.add(con);
                });
                long discoverEnd2 = System.currentTimeMillis();
                //如果只有一张表，直接通过此测试。
                boolean consumerFlag = true;
                int consumerErrorIndex = 0;
                int totalCount = 0;
                for (int consumerIndex = 0; consumerIndex < consumer2.size(); consumerIndex++) {
                    List<TapTable> tables = consumer2.get(consumerIndex);
                    if (consumerFlag) {
                        if (null == tables || tableCount < tables.size()) {
                            consumerErrorIndex = consumerIndex;
                            consumerFlag = false;
                        }
                    }
                    totalCount += null == tables ? 0 : tables.size();
                }
                boolean finalConsumerFlag = consumerFlag;
                int finalConsumerErrorIndex = consumerErrorIndex;

                int finalTotalCount = totalCount;
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                finalConsumerFlag,
                                LangUtil.format(
                                        "discoverByTableCount1.consumer.error",
                                        finalTotalCount,
                                        tableCount,
                                        tableCount,
                                        tableCount,
                                        finalConsumerErrorIndex + 1,
                                        consumer2.size() < finalConsumerErrorIndex + 1 || null == consumer2.get(finalConsumerErrorIndex) ? 0 : consumer2.get(finalConsumerErrorIndex).size(),
                                        discoverEnd2 - discoverStart2
                                )
                        )
                ).acceptAsError(testCase, LangUtil.format("discoverByTableCount1.consumer.succeed",
                        finalTotalCount,
                        tableCount,
                        tableCount,
                        tableCount,
                        consumer2.size(),
                        tableCount,
                        discoverEnd2 - discoverStart2
                ));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });

    }

    @DisplayName("discoverSchema.discoverByTableCount2")//用例6， 通过指定表数量加载固定数量的表（依赖CreateTableFunction）
    @Test
    @TapTestCase(sort = 6, dump = true)
    /**
     * 通过CreateTableFunction另外创建一张表，
     * 通过int tableSize参数指定为1，
     * 通过Consumer<List<TapTable>> consumer返回了一张表为成功。
     * 验证结束之后需要删掉随机建的表（依赖DropTableFunction）
     * */
    void discoverByTableCount2() {
        System.out.println(LangUtil.format("discoverSchema.discoverByTableCount2.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("discoverByTableCount2");
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();

                //通过CreateTableFunction另外创建一张表，
                if (!(hasCreateTable = this.createTable(prepare))) return;
                final String targetTableId = targetTable.getId();

                //通过int tableSize参数指定为1，
                int tableCount = 100;

                List<List<TapTable>> consumer = new ArrayList<>();

                long discoverStart = System.currentTimeMillis();
                //@TODO tableSize = tableCount
                connector.discoverSchema(connectorContext, new ArrayList<>(), tableCount, c -> {
                    if (null != c) consumer.add(c);
                });
                long discoverEnd = System.currentTimeMillis();

                boolean consumerFlag = true;
                int consumerErrorIndex = 0;
                int totalCount = 0;
                for (int consumerIndex = 0; consumerIndex < consumer.size(); consumerIndex++) {
                    List<TapTable> tables = consumer.get(consumerIndex);
                    if (consumerFlag) {
                        if (null == tables || tableCount < tables.size()) {
                            consumerErrorIndex = consumerIndex;
                            consumerFlag = false;
                        }
                    }
                    totalCount += null == tables ? 0 : tables.size();
                }
                boolean finalConsumerFlag = consumerFlag;
                int finalConsumerErrorIndex = consumerErrorIndex;
                int finalTotalCount = totalCount;
                //通过Consumer<List<TapTable>> consumer返回了一张表为成功。
                //如果只有一张表， 直接通过此测试。
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                finalConsumerFlag,
                                LangUtil.format(
                                        "discoverByTableCount2.consumer.error",
                                        1,
                                        targetTableId,
                                        tableCount,
                                        tableCount,
                                        finalTotalCount,
                                        finalConsumerErrorIndex + 1,
                                        null == consumer.get(finalConsumerErrorIndex) ? 0 : consumer.get(finalConsumerErrorIndex).size(),
                                        discoverEnd - discoverStart
                                )
                        )
                ).acceptAsError(testCase, LangUtil.format("discoverByTableCount2.consumer.succeed",
                        1,
                        targetTableId,
                        tableCount,
                        tableCount,
                        consumer.size(),
                        tableCount,
                        discoverEnd - discoverStart
                ));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                if (hasCreateTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                supportAny(list(
                        WriteRecordFunction.class, CreateTableFunction.class, CreateTableV2Function.class
                ), LangUtil.format(anyOneFunFormat, "WriteRecordFunction,CreateTableFunction,CreateTableV2Function"))
        );
    }
}
