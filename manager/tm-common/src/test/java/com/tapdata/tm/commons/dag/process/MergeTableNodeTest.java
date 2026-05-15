package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.util.JsonUtil;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MergeTableNodeTest {
    @Nested
    class createMapFieldTest{
        MergeTableNode mergeTableNode = new MergeTableNode();

        @DisplayName("test mergeType is updateIntoArray")
        @Test
        void testCreateArray(){
            Field result = mergeTableNode.createMapField("test","test", MergeTableProperties.MergeType.updateIntoArray);
            assertEquals("Array", result.getDataType());
            TapArray tapArray = new TapArray();
            tapArray.setType((byte) 2);
            assertEquals(JsonUtil.toJson(tapArray), result.getTapType());
        }

        @DisplayName("test mergeType is updateWrite")
        @Test
        void testCreateMap(){
            Field result = mergeTableNode.createMapField("test","test", MergeTableProperties.MergeType.updateWrite);
            assertEquals("Map", result.getDataType());
            TapMap tapMap = new TapMap();
            tapMap.setType((byte) 4);
            assertEquals(JsonUtil.toJson(tapMap), result.getTapType());
        }

    }
}
