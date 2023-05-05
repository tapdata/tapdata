package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TapAssertException;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.*;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * 都需使用随机ID建表， 如果有DropTableFunction实现， 测试用例应该自动删除创建的临时表（无论成功或是失败）
 * After的字段分为两种情况:
 * 一种是全字段都有的修改（不代表每个字段都改了， 只是里面部分字段修改），
 * 另外一种是只有部分修改字段， 这两种情况发送到目标端， 都能正常修改数据
 * 做一个有5个字段的数据， 类型各异， 数据源如果支持建表， 需要按这个5个字段建表， 插入1条这样的数据， 内容随意
 */
//After包含全字段修改或者不包含的情况下， 能正常修改数据（依赖WriteRecordFunction）
@DisplayName("fieldModification")
@TapGo(tag = "V3", sort = 10000)
public class FieldModificationTest extends PDKTestBaseV2 {
    //  构建一个修改事件， after里修改全部5个字段，
    //  如果实现了QueryByFilterFunction，
    //      就通过QueryByFilterFunction查询出来进行比值验证，
    //  如果没有实现， 输出没有做值比对， 但修改成功
    //用例1， 修改全部数据正常
    @DisplayName("fieldModification.all")
    @TapTestCase(sort = 1)
    @Test
    void modifyAll() throws NoSuchMethodException {
        Method testCase = getMethod("modifyAll");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                connectorOnStart(prepare);
                execute.testCase(testCase);
                //建表
                if (!(createTable = createTable(prepare))) {
                    return;
                }

                //插入1条数据
                final int recordCount = 1;
                Record[] records = Record.testRecordWithTapTable(targetTable, recordCount);
                WriteListResult<TapRecordEvent> insert = execute.builderRecord(records).insert();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != insert && insert.getInsertedCount() == recordCount,
                                langUtil.formatLang("fieldModification.all.insert.error", recordCount, null == insert ? 0 : insert.getInsertedCount(), recordCount)
                        )
                ).acceptAsError(testCase, langUtil.formatLang("fieldModification.all.insert.succeed", recordCount, null == insert ? 0 : insert.getInsertedCount()));

                Collection<String> primaryKeys = targetTable.primaryKeys(true);
                TapConnectorContext context = prepare.connectorNode().getConnectorContext();
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                QueryByFilterFunction queryByFilter = connectorFunctions.getQueryByFilterFunction();
                QueryByAdvanceFilterFunction advanceFilter = connectorFunctions.getQueryByAdvanceFilterFunction();

                // 修改1条数据
                Record.modifyRecordWithTapTable(targetTable, records, -1, false);
                WriteListResult<TapRecordEvent> update = execute.builderRecordCleanBefore(records).update();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != update && update.getModifiedCount() == recordCount,
                                langUtil.formatLang("fieldModification.all.update.error", recordCount, recordCount,
                                        null == update ? 0 : update.getInsertedCount(),
                                        null == update ? 0 : update.getModifiedCount(),
                                        null == update ? 0 : update.getRemovedCount()
                                )
                        )
                ).acceptAsError(testCase, langUtil.formatLang("fieldModification.all.update.succeed", recordCount, null == update ? 0 : update.getInsertedCount()));

                // 查询这些数据
                DataMap dataMap = DataMap.create();
                records = execute.records();
                for (Record record : records) {
                    for (String primaryKey : primaryKeys) {
                        dataMap.kv(primaryKey, record.get(primaryKey));
                    }
                }
                List<TapFilter> filters = new ArrayList<>();
                AtomicBoolean implementedFilter = new AtomicBoolean(false);
                List<Map<String, Object>> result = new ArrayList<>();
                TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                if (Objects.nonNull(queryByFilter)) {
                    implementedFilter.set(true);
                    TapFilter filter = new TapFilter();
                    filter.setMatch(dataMap);
                    filters.add(filter);
                    queryByFilter.query(context, filters, targetTableModel, consumer -> {
                        if (Objects.nonNull(consumer) && !consumer.isEmpty()) {
                            consumer.forEach(res -> result.add(transform(prepare, targetTableModel, res.getResult())));
                        }
                    });
                } else {
                    Optional.ofNullable(advanceFilter).ifPresent(filter -> {
                        implementedFilter.set(true);
                        TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
                        tapAdvanceFilter.match(dataMap);
                        try {
                            filter.query(context, tapAdvanceFilter, targetTableModel, consumer -> {
                                for (Map<String, Object> data : consumer.getResults()) {
                                    result.add(transform(prepare, targetTableModel, data));
                                }
                            });
                        } catch (Throwable throwable) {
                            TapAssert.error(testCase, langUtil.formatLang("fieldModification.all.query.throw", throwable.getMessage()));
                        }
                    });
                }

                if (implementedFilter.get()) {
                    int size = result.size();
                    if (size != recordCount) {
                        TapAssert.error(testCase, langUtil.formatLang("fieldModification.all.query.fail", recordCount, size));
                    } else {
                        Map<String, Object> resultMap = result.get(0);
                        StringBuilder builder = new StringBuilder();
                        ConnectorNode connectorNode = prepare.connectorNode();
                        //connectorNode.getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
                        //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);
                        boolean equals = mapEquals(transform(prepare, targetTableModel, records[0]), resultMap, builder, targetTableModel.getNameFieldMap());
                        TapAssert.asserts(() -> {
                            Assertions.assertTrue(equals, langUtil.formatLang("fieldModification.all.query.error", recordCount, size, builder.toString()));
                        }).acceptAsWarn(testCase, langUtil.formatLang("fieldModification.all.query.succeed", recordCount, size, builder.toString()));
                    }
                } else {
                    //没有实现QueryByFilterFunction 和 QueryByAdvanceFilterFunction
                    TapAssert.warn(testCase, langUtil.formatLang("fieldModification.not.filter"));
                }
            } catch (Throwable e) {
                if (!(e instanceof TapAssertException))
                    TapAssert.error(testCase, langUtil.formatLang("fieldModification.all.throw", e.getMessage()));
            } finally {
                if (createTable) execute.dropTable();
                connectorOnStop(prepare);
            }
        });
    }


    //  构建一个修改事件， after里修改5个字段中的两个字段，
    //  如果实现了QueryByFilterFunction，
    //      就通过QueryByFilterFunction查询出来进行比值验证，
    //  如果没有实现， 输出没有做值比对， 但修改成功
    //用例2， 修改部分数据正常
    @DisplayName("fieldModification.part")
    @TapTestCase(sort = 2)
    @Test
    void modifyPart() throws NoSuchMethodException {
        Method testCase = getMethod("modifyPart");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                connectorOnStart(prepare);
                execute.testCase(testCase);

                //建表
                if (!(createTable = createTable(prepare))) {
                    return;
                }
                //插入5条数据
                final int recordCount = 1;
                final int modifyCount = 2;
                Record[] records = Record.testRecordWithTapTable(targetTable, recordCount);
                WriteListResult<TapRecordEvent> insert = execute.builderRecord(records).insert();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != insert && insert.getInsertedCount() == recordCount,
                                langUtil.formatLang("fieldModification.all.insert.error", recordCount, null == insert ? 0 : insert.getInsertedCount(), recordCount)
                        )
                ).acceptAsError(testCase, langUtil.formatLang("fieldModification.all.insert.succeed", recordCount, null == insert ? 0 : insert.getInsertedCount()));

                Collection<String> primaryKeys = targetTable.primaryKeys(true);
                TapConnectorContext context = prepare.connectorNode().getConnectorContext();
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                QueryByFilterFunction queryByFilter = connectorFunctions.getQueryByFilterFunction();
                QueryByAdvanceFilterFunction advanceFilter = connectorFunctions.getQueryByAdvanceFilterFunction();

                // 修改1条数据
                Record.modifyRecordWithTapTable(targetTable, records, modifyCount, false);
                WriteListResult<TapRecordEvent> update = execute.builderRecordCleanBefore(records).update();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != update && update.getModifiedCount() == recordCount,
                                langUtil.formatLang("fieldModification.some.update.error",
                                        recordCount,
                                        modifyCount,
                                        recordCount,
                                        null == update ? 0 : update.getInsertedCount(),
                                        null == update ? 0 : update.getModifiedCount(),
                                        null == update ? 0 : update.getRemovedCount()
                                )
                        )
                ).acceptAsError(testCase, langUtil.formatLang("fieldModification.some.update.succeed", recordCount, modifyCount));

                // 查询这些数据
                DataMap dataMap = DataMap.create();
                records = execute.records();
                for (Record record : records) {
                    for (String primaryKey : primaryKeys) {
                        dataMap.kv(primaryKey, record.get(primaryKey));
                    }
                }
                List<TapFilter> filters = new ArrayList<>();
                AtomicBoolean implementedFilter = new AtomicBoolean(false);
                List<Map<String, Object>> result = new ArrayList<>();

                if (Objects.nonNull(queryByFilter)) {
                    implementedFilter.set(true);
                    TapFilter filter = new TapFilter();
                    filter.setMatch(dataMap);
                    filters.add(filter);
                    TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                    queryByFilter.query(context, filters, targetTableModel, consumer -> {
                        if (Objects.nonNull(consumer) && !consumer.isEmpty()) {
                            consumer.forEach(res -> result.add(transform(prepare, targetTableModel, res.getResult())));
                        }
                    });
                } else {
                    Optional.ofNullable(advanceFilter).ifPresent(filter -> {
                        implementedFilter.set(true);
                        TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
                        tapAdvanceFilter.match(dataMap);
                        try {
                            TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                            filter.query(context, tapAdvanceFilter, targetTableModel, consumer -> {
                                for (Map<String, Object> data : consumer.getResults()) {
                                    result.add(transform(prepare, targetTableModel, data));
                                }
                            });
                        } catch (Throwable throwable) {
                            TapAssert.error(testCase, langUtil.formatLang("fieldModification.some.query.throw",
                                    recordCount,
                                    modifyCount,
                                    throwable.getMessage()));
                        }
                    });
                }

                if (implementedFilter.get()) {
                    int size = result.size();
                    if (size != recordCount) {
                        TapAssert.error(testCase, langUtil.formatLang("fieldModification.some.query.fail",
                                recordCount,
                                modifyCount,
                                size));
                    } else {
                        Map<String, Object> resultMap = result.get(0);
                        StringBuilder builder = new StringBuilder();
                        TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                        //prepare.connectorNode().getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
                        //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);
                        boolean equals = mapEquals(transform(prepare, targetTableModel, records[0]), resultMap, builder, targetTableModel.getNameFieldMap());
                        TapAssert.asserts(() -> {
                            Assertions.assertTrue(equals, langUtil.formatLang("fieldModification.some.query.error",
                                    recordCount,
                                    modifyCount,
                                    size,
                                    builder.toString()));
                        }).acceptAsWarn(testCase, langUtil.formatLang("fieldModification.some.query.succeed",
                                recordCount,
                                modifyCount,
                                size
                        ));
                    }
                } else {
                    //没有实现QueryByFilterFunction 和 QueryByAdvanceFilterFunction
                    TapAssert.warn(testCase, langUtil.formatLang("fieldModification.not.filter"));
                }
            } catch (Throwable e) {
                if (!(e instanceof TapAssertException))
                    TapAssert.error(testCase, langUtil.formatLang("fieldModification.all.throw", e.getMessage()));
            } finally {
                if (createTable) execute.dropTable();
                connectorOnStop(prepare);
            }
        });
    }

    //  构建一个修改事件， after里修改5个字段中的两个字段，
    //  如果实现了QueryByFilterFunction，
    //      就通过QueryByFilterFunction查询出来进行比值验证，
    //  如果没有实现， 输出没有做值比对， 但修改成功
    //用例2， 修改部分数据正常
    @DisplayName("fieldModification.part")
    @TapTestCase(sort = 3)
    @Test
    void modifyPartV2() throws NoSuchMethodException {
        Method testCase = getMethod("modifyPartV2");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                connectorOnStart(prepare);
                execute.testCase(testCase);

                //建表
                if (!(createTable = createTable(prepare))) {
                    return;
                }
                //插入5条数据
                final int recordCount = 1;
                final int modifyCount = 2;
                Record[] records = Record.testRecordWithTapTable(targetTable, recordCount);
                WriteListResult<TapRecordEvent> insert = execute.builderRecord(records).insert();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != insert && insert.getInsertedCount() == recordCount,
                                langUtil.formatLang("fieldModification.all.insert.error", recordCount, null == insert ? 0 : insert.getInsertedCount(), recordCount)
                        )
                ).acceptAsError(testCase, langUtil.formatLang("fieldModification.all.insert.succeed", recordCount, null == insert ? 0 : insert.getInsertedCount()));

                Collection<String> primaryKeys = targetTable.primaryKeys(true);
                TapConnectorContext context = prepare.connectorNode().getConnectorContext();
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                QueryByFilterFunction queryByFilter = connectorFunctions.getQueryByFilterFunction();
                QueryByAdvanceFilterFunction advanceFilter = connectorFunctions.getQueryByAdvanceFilterFunction();

                // 修改1条数据中的部分数据，并只保留修改的部分
                records = Record.modifyAsNewRecordWithTapTable(targetTable, records, modifyCount, false);
                execute.builderRecordCleanBefore(records);
                WriteListResult<TapRecordEvent> update = execute.builderRecordCleanBefore(records).update();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != update && update.getModifiedCount() == recordCount,
                                langUtil.formatLang("fieldModification.some2.update.error",
                                        recordCount,
                                        modifyCount,
                                        recordCount,
                                        null == update ? 0 : update.getInsertedCount(),
                                        null == update ? 0 : update.getModifiedCount(),
                                        null == update ? 0 : update.getRemovedCount()
                                )
                        )
                ).acceptAsError(testCase, langUtil.formatLang("fieldModification.some2.update.succeed",
                        recordCount,
                        modifyCount));

                // 查询这些数据
                records = execute.records();
                DataMap dataMap = DataMap.create();
                for (Record record : records) {
                    for (String primaryKey : primaryKeys) {
                        dataMap.kv(primaryKey, record.get(primaryKey));
                    }
                }
                List<TapFilter> filters = new ArrayList<>();
                AtomicBoolean implementedFilter = new AtomicBoolean(false);
                List<Map<String, Object>> result = new ArrayList<>();

                if (Objects.nonNull(queryByFilter)) {
                    implementedFilter.set(true);
                    TapFilter filter = new TapFilter();
                    filter.setMatch(dataMap);
                    filters.add(filter);
                    TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                    queryByFilter.query(context, filters, targetTableModel, consumer -> {
                        if (Objects.nonNull(consumer) && !consumer.isEmpty()) {
                            consumer.forEach(res -> {
                                result.add(transform(prepare, targetTableModel, res.getResult()));
                            });
                        }
                    });
                } else {
                    Optional.ofNullable(advanceFilter).ifPresent(filter -> {
                        implementedFilter.set(true);
                        TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
                        tapAdvanceFilter.match(dataMap);
                        try {
                            TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                            filter.query(context, tapAdvanceFilter, targetTableModel, consumer -> {
                                for (Map<String, Object> data : consumer.getResults()) {
                                    result.add(transform(prepare, targetTableModel, data));
                                }
                            });
                        } catch (Throwable throwable) {
                            TapAssert.error(testCase, langUtil.formatLang("fieldModification.some2.query.throw",
                                    recordCount,
                                    modifyCount,
                                    throwable.getMessage()));
                        }
                    });
                }

                if (implementedFilter.get()) {
                    int size = result.size();
                    if (size != recordCount) {
                        TapAssert.error(testCase, langUtil.formatLang("fieldModification.some2.query.fail",
                                recordCount,
                                modifyCount,
                                size));
                    } else {
                        Map<String, Object> resultMap = result.get(0);
                        StringBuilder builder = new StringBuilder();
                        TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                        //prepare.connectorNode().getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
                        //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);
                        boolean equals = mapEquals(transform(prepare, targetTableModel, records[0]), resultMap, builder, targetTableModel.getNameFieldMap());
                        TapAssert.asserts(() -> {
                            Assertions.assertTrue(equals, langUtil.formatLang("fieldModification.some2.query.error",
                                    recordCount,
                                    modifyCount,
                                    size,
                                    builder.toString()));
                        }).acceptAsWarn(testCase, langUtil.formatLang("fieldModification.some2.query.succeed",
                                recordCount,
                                modifyCount,
                                size));
                    }
                } else {
                    //没有实现QueryByFilterFunction 和 QueryByAdvanceFilterFunction
                    TapAssert.warn(testCase, langUtil.formatLang("fieldModification.not.filter"));
                }
            } catch (Throwable e) {
                if (!(e instanceof TapAssertException))
                    TapAssert.error(testCase, langUtil.formatLang("fieldModification.all.throw", e.getMessage()));
            } finally {
                if (createTable) execute.dropTable();
                connectorOnStop(prepare);
            }
        });
    }


    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, langUtil.formatLang(inNeedFunFormat, "WriteRecordFunction"))
        );
    }
}

//interface Modify{
//    public void modify();
//}
