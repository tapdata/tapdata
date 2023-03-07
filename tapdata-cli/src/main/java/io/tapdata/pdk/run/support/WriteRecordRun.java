package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.RunClassMap;
import io.tapdata.pdk.run.base.RunnerSummary;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;

@DisplayName("writeRecordRun")
@TapGo(sort = 7)
public class WriteRecordRun extends PDKBaseRun {
    public static final String writerName = RunClassMap.WRITE_RECORD_RUN.jsName(0);
    public static final String insertName = RunClassMap.WRITE_RECORD_RUN.jsName(1);
    public static final String updateName = RunClassMap.WRITE_RECORD_RUN.jsName(2);
    public static final String deleteName = RunClassMap.WRITE_RECORD_RUN.jsName(3);
    @DisplayName("writeRecordRun.run")
    @TapTestCase(sort = 1)
    @Test
    void writeRecord() throws NoSuchMethodException {
        Method testCase = super.getMethod("writeRecord");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                WriteRecordFunction write = functions.getWriteRecordFunction();
                Map<String, Object> writeRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(WriteRecordRun.writerName)).orElse(new HashMap<String, Object>());
                Map<String, Object> insertRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(WriteRecordRun.insertName)).orElse(new HashMap<String, Object>());
                Map<String, Object> deleteRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(WriteRecordRun.deleteName)).orElse(new HashMap<String, Object>());
                Map<String, Object> updateRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(WriteRecordRun.updateName)).orElse(new HashMap<String, Object>());

                List<Map<String, Object>> eventDataList = new ArrayList<>();
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(writeRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(insertRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(deleteRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(updateRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));

                Map<String, List<Map<String, Object>>> stringListMap = eventDataList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(map -> String.valueOf(map.get("tableName"))));
                Map<String, WriteListResult<TapRecordEvent>> eventResult = new HashMap<>();
                for (Map.Entry<String, List<Map<String, Object>>> entry : stringListMap.entrySet()) {
                    String tableName = entry.getKey();
                    TapTable table = new TapTable(tableName, tableName);
                    write.writeRecord(context, this.convertTapEvent(entry.getValue()), table, events -> {
                        String tableId = table.getId();
                        WriteListResult<TapRecordEvent> recordEvents = eventResult.computeIfAbsent(tableId, key -> new WriteListResult<TapRecordEvent>());
                        recordEvents.setRemovedCount(events.getRemovedCount());
                        recordEvents.modifiedCount(events.getModifiedCount());
                        recordEvents.insertedCount(events.getInsertedCount());
                        recordEvents.setErrorMap(events.getErrorMap());
                        eventResult.put(tableId, recordEvents);
                    });
                    super.runSucceed(testCase, RunnerSummary.format("formatValue",super.formatPatten(eventResult)));
                }
            } catch (Throwable throwable) {
                super.runError(testCase, RunnerSummary.format("formatValue",throwable.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    private List<TapRecordEvent> convertTapEvent(List<Map<String, Object>> events) {
        List<TapRecordEvent> eventList = new ArrayList<>();
        if (Objects.nonNull(events) && !events.isEmpty()) {
            events.stream().filter(Objects::nonNull).forEach(e -> {
                String eventType = String.valueOf(Optional.ofNullable(e.get("eventType")).orElse("i"));
                Map<String, Object> afterData = (Map<String, Object>) (Optional.ofNullable(e.get("afterData")).orElse(new HashMap<>()));
                Map<String, Object> beforeData = (Map<String, Object>) (Optional.ofNullable(e.get("beforeData")).orElse(new HashMap<>()));
                Long referenceTime = Long.parseLong(String.valueOf(Optional.ofNullable(e.get("referenceTime")).orElse(System.currentTimeMillis())));
                String tableName = String.valueOf(Optional.ofNullable(e.get("tableName")).orElse("i"));
                TapRecordEvent event;
                switch (eventType) {
                    case "u": {
                        event = updateDMLEvent(beforeData, afterData, tableName).referenceTime(referenceTime);
                    }
                    break;
                    case "d": {
                        event = deleteDMLEvent(beforeData, tableName).referenceTime(referenceTime);
                    }
                    break;
                    default: {
                        event = insertRecordEvent(afterData, tableName).referenceTime(referenceTime);
                    }
                }
                eventList.add(event);
            });
        }
        return eventList;
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(WriteRecordFunction.class, RunnerSummary.format("jsFunctionInNeed",
                String.format("any one or more of %s and %s and %s and %s",
                        WriteRecordRun.writerName,
                        WriteRecordRun.insertName,
                        WriteRecordRun.updateName,
                        WriteRecordRun.deleteName
                ))));
    }
}
