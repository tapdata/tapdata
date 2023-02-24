package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.*;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
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
@TapGo(tag = "V3", sort = 13)
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
        Method testCase = super.getMethod("modifyAll");
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                //建表
                if (!(createTable = super.createTable(prepare))) {
                    return;
                }

                //插入1条数据
                final int recordCount = 1;
                Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);
                WriteListResult<TapRecordEvent> insert = execute.builderRecord(records).insert();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != insert && insert.getInsertedCount() == recordCount,
                                langUtil.formatLang("fieldModification.all.insert.error", recordCount, null == insert ? 0 : insert.getInsertedCount())
                        )
                ).acceptAsError(testCase, LangUtil.format("fieldModification.all.insert.succeed", recordCount, null == insert ? 0 : insert.getInsertedCount()));

                Collection<String> primaryKeys = super.targetTable.primaryKeys(true);
                TapConnectorContext context = prepare.connectorNode().getConnectorContext();
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                QueryByFilterFunction queryByFilter = connectorFunctions.getQueryByFilterFunction();
                QueryByAdvanceFilterFunction advanceFilter = connectorFunctions.getQueryByAdvanceFilterFunction();

                // 修改1条数据
                Record.modifyRecordWithTapTable(super.targetTable, records, -1, false);
                WriteListResult<TapRecordEvent> update = execute.builderRecord(records).update();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != update && update.getInsertedCount() == recordCount,
                                langUtil.formatLang("fieldModification.all.update.error", recordCount, null == update ? 0 : update.getInsertedCount())
                        )
                ).acceptAsError(testCase, LangUtil.format("fieldModification.all.update.succeed", recordCount, null == update ? 0 : update.getInsertedCount()));

                // 查询这些数据
                DataMap dataMap = DataMap.create();
                for (Record record : records) {
                    for (String primaryKey : primaryKeys) {
                        dataMap.kv(primaryKey, record.get(primaryKey));
                    }
                }
                List<TapFilter> filters = new ArrayList<>();
                AtomicBoolean implementedFilter = new AtomicBoolean(false);
                List<Map<String,Object>> result = new ArrayList<>();

                if (Objects.nonNull(queryByFilter)) {
                    implementedFilter.set(true);
                    TapFilter filter = new TapFilter();
                    filter.setMatch(dataMap);
                    queryByFilter.query(context, filters, super.targetTable, consumer -> {
                        if (Objects.nonNull(consumer) && !consumer.isEmpty()) {
                            consumer.forEach(res-> result.add(res.getResult()));
                        }
                    });
                } else {
                    Optional.ofNullable(advanceFilter).ifPresent(filter -> {
                        implementedFilter.set(true);
                        TapAdvanceFilter tapAdvanceFilter = new TapAdvanceFilter();
                        tapAdvanceFilter.match(dataMap);
                        try {
                            filter.query(context, tapAdvanceFilter, super.targetTable, consumer -> {
                                result.addAll(consumer.getResults());
                            });
                        } catch (Throwable throwable) {
                            TapAssert.error(testCase,"QueryByAdvanceFilterFunction 抛出了一个异常，error: %s.");
                        }
                    });
                }

                if (implementedFilter.get()) {
                    if (result.size() != recordCount) {
                        TapAssert.error(testCase, "插入了一条数据，但是实际返回多条。");
                    } else {
                        Map<String, Object> resultMap = result.get(0);
                        StringBuilder builder = new StringBuilder();
                        boolean equals = super.mapEquals((Map<String, Object>) records[0], resultMap, builder);
                        TapAssert.asserts(() -> {
                            Assertions.assertTrue(equals, "数据前后对比不一致，对比结果为：%s");
                        }).acceptAsError(testCase, "数据前后对比一致，对比结果一致");
                    }
                }else {
                    //没有实现QueryByFilterFunction 和 QueryByAdvanceFilterFunction
                    TapAssert.warn(testCase, langUtil.formatLang(""));
                }
            } catch (Throwable e) {
                TapAssert.error(testCase, langUtil.formatLang("fieldModification.all.throw", e.getMessage()));
            } finally {
                if (createTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }


    //  构建一个修改事件， after里修改5个字段中的两个字段，
    //  如果实现了QueryByFilterFunction，
    //      就通过QueryByFilterFunction查询出来进行比值验证，
    //  如果没有实现， 输出没有做值比对， 但修改成功
    //用例2， 修改部分数据正常
    @DisplayName("fieldModification.part")
    @TapTestCase(sort = 1)
    @Test
    void modifyPart() throws NoSuchMethodException {
        Method testCase = super.getMethod("modifyPart");
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                //建表

                //插入5条数据

                //修改5条数据

                //查询这些数据

                //对比这些数据

            } catch (Throwable e) {
                TapAssert.error(testCase, langUtil.formatLang("fieldModification.part.throw", e.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    //  构建一个修改事件， after里修改5个字段中的两个字段，
    //  如果实现了QueryByFilterFunction，
    //      就通过QueryByFilterFunction查询出来进行比值验证，
    //  如果没有实现， 输出没有做值比对， 但修改成功
    //用例2， 修改部分数据正常
    @DisplayName("fieldModification.part")
    @TapTestCase(sort = 1)
    @Test
    void modifyPartV2() throws NoSuchMethodException {
        Method testCase = super.getMethod("modifyPartV2");
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                //建表

                //插入5条数据

                //修改5条数据

                //查询这些数据

                //对比这些数据

            } catch (Throwable e) {
                TapAssert.error(testCase, langUtil.formatLang("fieldModification.part.throw", e.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }


    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, langUtil.formatLang(inNeedFunFormat, "WriteRecordFunction"))
        );
    }
}
