package com.tapdata.tm.trace.service.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.dto.TraceFieldMapping;
import com.tapdata.tm.trace.dto.TraceStreamEvent;
import com.tapdata.tm.trace.param.WideTableTraceRequest;
import com.tapdata.tm.trace.service.bloodline.BloodlineFinder;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TraceDataServiceTest {

    @Test
    void traceData_shouldFallbackTracedFieldOriginToTrackedField_whenTargetOriginNameBlank() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        BloodlineFinder bloodlineFinder = mock(BloodlineFinder.class);
        TraceDataService service = new TraceDataService();
        ReflectionTestUtils.setField(service, "objectMapper", objectMapper);
        ReflectionTestUtils.setField(service, "bloodlineFinder", bloodlineFinder);

        LineageTableNode target = new LineageTableNode("orders", "connA", "connectionA", "pdk", null);
        target.setId("target");
        Dag dag = new Dag();
        dag.setNodes(new ArrayList<>(List.of(target)));
        dag.setEdges(new ArrayList<>());

        TaskLineageDto lineage = new TaskLineageDto(dag);
        lineage.setFieldNameMapping(Map.of("target", Map.of("fieldA", "")));
        lineage.setUpdateConditionFieldList(Collections.emptyMap());
        when(bloodlineFinder.findTaskLineage(any())).thenReturn(lineage);

        WideTableTraceRequest request = new WideTableTraceRequest();
        request.setConnectionId("connA");
        request.setTable("orders");
        request.setTrackedFields(List.of("fieldA"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        service.traceData(request, "request-1", outputStream);

        String firstEvent = outputStream.toString(StandardCharsets.UTF_8).lines().findFirst().orElseThrow();
        TraceStreamEvent event = objectMapper.readValue(firstEvent, TraceStreamEvent.class);
        TraceFieldMapping tracedField = event.getTraceValue().getTracedFields().get(0);
        assertEquals("fieldA", tracedField.getOriginName());
        assertEquals("fieldA", tracedField.getCurrentName());
    }
}
