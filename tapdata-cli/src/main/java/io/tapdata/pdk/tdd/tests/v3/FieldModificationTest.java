package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
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
import io.tapdata.pdk.tdd.tests.support.*;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                                super.langUtil.formatLang("fieldModification.all.insert.error", recordCount, null == insert ? 0 : insert.getInsertedCount())
                        )
                ).acceptAsError(testCase, LangUtil.format("fieldModification.all.insert.succeed", recordCount, null == insert ? 0 : insert.getInsertedCount()));

                Collection<String> primaryKeys = super.targetTable.primaryKeys(true);
                TapConnectorContext context = prepare.connectorNode().getConnectorContext();
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                QueryByFilterFunction queryByFilter = connectorFunctions.getQueryByFilterFunction();
                QueryByAdvanceFilterFunction advanceFilter = connectorFunctions.getQueryByAdvanceFilterFunction();

                //@TODO 插入后查询出是否相同

                //@TODO 修改1条数据
                Record.modifyRecordWithTapTable(super.targetTable, records, -1,false);
                WriteListResult<TapRecordEvent> update = execute.builderRecord(records).update();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != update && update.getInsertedCount() == recordCount,
                                super.langUtil.formatLang("fieldModification.all.update.error", recordCount, null == update ? 0 : update.getInsertedCount())
                        )
                ).acceptAsError(testCase, LangUtil.format("fieldModification.all.update.succeed", recordCount, null == update ? 0 : update.getInsertedCount()));

                //@TODO 查询这些数据
                if (Objects.nonNull(queryByFilter)){
                    List<TapFilter> filters = new ArrayList<>();
                    TapFilter filter = new TapFilter();
                    DataMap dataMap = DataMap.create();
                    for (Record record : records) {
                        for (String primaryKey : primaryKeys) {
                            dataMap.kv(primaryKey,record.get(primaryKey));
                        }
                    }
                    filter.setMatch(dataMap);
                    List<FilterResult> result = new ArrayList<>();
                    queryByFilter.query(context,filters,super.targetTable,consumer->{
                        if (Objects.nonNull(consumer) && !consumer.isEmpty()) {
                            result.addAll(consumer);
                        }
                    });

                    for (FilterResult filterResult : result) {
                        Map<String, Object> filterKV = filterResult.getResult();

                        //@TODO 对比结构
                        if (null != filterKV && filterKV.size() == 1) {
                            Record record = records[0];
                            Map<String, Object> tapEvent = filterResult.getResult();
                            ConnectorNode connectorNode = prepare.connectorNode();
                            TapTable targetTable = connectorNode.getConnectorContext().getTableMap().get(connectorNode.getTable());
                            connectorNode.getCodecsFilterManager().transformToTapValueMap(tapEvent, targetTable.getNameFieldMap());
                            connectorNode.getCodecsFilterManager().transformFromTapValueMap(tapEvent);
                            StringBuilder builder = new StringBuilder();
                            TapAssert.asserts(() -> assertTrue(
                                    mapEquals(record, tapEvent, builder),
                                    LangUtil.format("exact.equals.failed", recordCount, builder.toString())
                            )).acceptAsWarn(testCase, LangUtil.format("exact.equals.succeed", recordCount, builder.toString()));
                        }

                        //再进行模糊值匹配，只要能查出来数据就算是正确。如果值只能通过模糊匹配成功，报警告指出是靠模糊匹配成功的，
                        //如果连模糊匹配也匹配不上，也报警告（值对与不对很多时候不好判定）。
                        filters = new ArrayList<>();
                        TapAdvanceFilter tapAdvanceFilter = TapAdvanceFilter.create();
                        filters.add(tapAdvanceFilter);
                        queryByFilter.query(
                                prepare.connectorNode().getConnectorContext(),
                                filters,
                                targetTable,
                                filterResults -> TapAssert.asserts(() -> {
                                }).acceptAsError(testCase, LangUtil.format(""))
                        );

                        //再进行精确，只要能查出来数据就算是正确。
                        filters = new ArrayList<>();
                        queryByFilter.query(
                                prepare.connectorNode().getConnectorContext(),
                                filters,
                                targetTable,
                                filterResults -> TapAssert.asserts(() -> {
                                }).acceptAsError(testCase, LangUtil.format(""))
                        );
                    }
                }else {
                    //没有实现QueryByFilterFunction
                    TapAssert.warn(testCase,super.langUtil.formatLang(""));
                }

                //@TODO 对比这些数据

            } catch (Throwable e) {
                TapAssert.error(testCase, super.langUtil.formatLang("fieldModification.all.throw", e.getMessage()));
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
                TapAssert.error(testCase, super.langUtil.formatLang("fieldModification.part.throw", e.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }


    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }
}
