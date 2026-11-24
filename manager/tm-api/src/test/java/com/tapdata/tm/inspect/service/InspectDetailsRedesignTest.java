package com.tapdata.tm.inspect.service;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Stats;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipOutputStream;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class InspectDetailsRedesignTest {
    @Test void customQueryExportRetainsKeyAndFieldMetadata() throws Exception {
        InspectDetailsService service = mock(InspectDetailsService.class, CALLS_REAL_METHODS);
        InspectResultService results = mock(InspectResultService.class);
        InspectDetailsDto request = new InspectDetailsDto();
        ObjectId resultId = new ObjectId();
        request.setInspectResultId(resultId.toHexString());
        request.setFullField(false);
        InspectDetailsDto detail = new InspectDetailsDto();
        detail.setTaskId("query-task");
        detail.setDifferenceType("VALUE_MISMATCH");
        detail.setResultKey(Map.of("id", 1));
        detail.setSourceField("total");
        detail.setTargetField("amount");
        detail.setSource(Map.of("id", 1, "total", 10));
        detail.setTarget(Map.of("id", 1, "amount", 20));
        Stats stats = new Stats();
        stats.setTaskId(detail.getTaskId());
        stats.setResult("failed");
        Source source = new Source();
        source.setTable("source");
        source.setConnectionName("db");
        stats.setSource(source);
        stats.setTarget(source);
        stats.setSourceTotal(1L);
        stats.setTargetTotal(1L);
        stats.setSourceOnly(0L);
        stats.setTargetOnly(0L);
        stats.setRowFailed(1L);
        InspectResultDto result = new InspectResultDto();
        result.setStats(List.of(stats));
        when(results.findById(resultId)).thenReturn(result);
        doReturn(List.of(detail)).when(service).findAll(any(org.springframework.data.mongodb.core.query.Query.class));
        ZipOutputStream stream = mock(ZipOutputStream.class);
        service.export(request, stream, null, results);
        ArgumentCaptor<byte[]> bytes = ArgumentCaptor.forClass(byte[].class);
        verify(stream).write(bytes.capture());
        JSONObject exported = JSONArray.parseArray(new String(bytes.getValue(), StandardCharsets.UTF_8))
                .getJSONObject(0).getJSONArray("data").getJSONObject(0);
        assertEquals("VALUE_MISMATCH", exported.getString("differenceType"));
        assertEquals(1, exported.getJSONObject("resultKey").getIntValue("id"));
        assertEquals("total", exported.getString("sourceField"));
        assertEquals("amount", exported.getString("targetField"));
        assertEquals(Map.of("total", 10), exported.getJSONObject("source"));
        assertEquals(Map.of("amount", 20), exported.getJSONObject("target"));
    }
    @Test void keyedFieldDifferenceExportUsesIndependentSourceAndTargetAliases() {
        InspectDetailsService service=mock(InspectDetailsService.class,CALLS_REAL_METHODS);
        InspectDetailsDto detail=new InspectDetailsDto();detail.setDifferenceType("VALUE_MISMATCH");
        detail.setSourceField("total");detail.setTargetField("amount");
        detail.setSource(new HashMap<>(Map.of("id",1,"total",10,"helper",99)));
        detail.setTarget(new HashMap<>(Map.of("id",1,"amount",20,"helper",99)));
        service.compareDifferenceFields(detail,null);
        assertEquals(Map.of("total",10),detail.getSource());assertEquals(Map.of("amount",20),detail.getTarget());
    }
    @Test void oneSidedRowsKeepAllTheirDifferingFields() {
        InspectDetailsService service=mock(InspectDetailsService.class,CALLS_REAL_METHODS);
        InspectDetailsDto detail=new InspectDetailsDto();detail.setDifferenceType("SOURCE_ONLY");detail.setSource(Map.of("id",1,"v",2));
        service.compareDifferenceFields(detail,null);assertEquals(Map.of("id",1,"v",2),detail.getSource());assertNull(detail.getTarget());
    }
}
