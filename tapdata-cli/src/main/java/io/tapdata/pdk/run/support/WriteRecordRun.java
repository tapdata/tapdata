package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.RunnerSummary;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;

@DisplayName("writeRecordRun")
@TapGo(sort = 7)
public class WriteRecordRun extends PDKBaseRun {
    @DisplayName("writeRecordRun.run")
    @TapTestCase(sort = 1)
    @Test
    void writeRecord() throws NoSuchMethodException {
        Method testCase = super.getMethod("writeRecord");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
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
                Map<String, Object> writeRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get("write_record")).orElse(new HashMap<String, Object>());
                Map<String, Object> insertRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get("insert_record")).orElse(new HashMap<String, Object>());
                Map<String, Object> deleteRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get("delete_record")).orElse(new HashMap<String, Object>());
                Map<String, Object> updateRecordConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get("update_record")).orElse(new HashMap<String, Object>());

                List<Map<String, Object>> eventDataList = new ArrayList<>();
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(writeRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(insertRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(deleteRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));
                eventDataList.addAll((List<Map<String, Object>>) Optional.ofNullable(updateRecordConfig.get("eventDataList")).orElse(new ArrayList<Map<String, Object>>()));

                Map<String, List<Map<String, Object>>> stringListMap = eventDataList.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(map -> String.valueOf(map.get("table_name"))));
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
                String eventType = String.valueOf(Optional.ofNullable(e.get("event_type")).orElse("i"));
                Map<String, Object> afterData = (Map<String, Object>) (Optional.ofNullable(e.get("after_data")).orElse(new HashMap<>()));
                Map<String, Object> beforeData = (Map<String, Object>) (Optional.ofNullable(e.get("before_data")).orElse(new HashMap<>()));
                Long referenceTime = Long.parseLong(String.valueOf(Optional.ofNullable(e.get("reference_time")).orElse(System.currentTimeMillis())));
                String tableName = String.valueOf(Optional.ofNullable(e.get("table_name")).orElse("i"));
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
        return list(support(WriteRecordFunction.class, RunnerSummary.format("jsFunctionInNeed","any one or more of writer_record and insert_record and update_record and delete_record")));
    }
}
