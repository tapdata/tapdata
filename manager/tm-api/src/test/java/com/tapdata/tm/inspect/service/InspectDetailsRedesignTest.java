package com.tapdata.tm.inspect.service;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class InspectDetailsRedesignTest {
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
