package com.tapdata.tm.metadatainstance;

import com.tapdata.tm.metadatainstance.controller.MetadataInstancesController;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Class MetadataInstancesController Test")
public class MetadataInstancesControllerTest {

    @Test
    void testConvertTapTypeToMap() {
        MetadataInstancesController metadataInstancesController = new MetadataInstancesController();
        assertNull(metadataInstancesController.convertTapTypeBytesToString(null));

        Map<String, TapType> tapTypeMap = new HashMap<>();
        tapTypeMap.put("varchar", new TapString(10L, true));
        tapTypeMap.put("null", null);
        tapTypeMap.put("int", new TapNumber().bit(10).maxValue(new BigDecimal("1000000")).precision(6).scale(0));
        tapTypeMap.put("binary", new TapBinary().bytes(10000L).byteRatio(3));
        tapTypeMap.put("jsonb", new TapString().bytes(9223372036854775807L));
        Map<String, Object> result = metadataInstancesController.convertTapTypeBytesToString(tapTypeMap);
        assertNull(result.get("null"));
        assertEquals(10, ((Map)result.get("int")).size());
        assertEquals("10000", ((Map)result.get("binary")).get("bytes"));
        assertEquals("9223372036854775807", ((Map)result.get("jsonb")).get("bytes"));
    }
}
