package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.schema.*;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapPartitionField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.simplify.TapSimplify;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PdkSchemaConvertTest {

    @Test
    void testToPdk1_tableAttrValueNull() {
        // Setup
        final MetadataInstancesDto schema = new MetadataInstancesDto();
        schema.setMetaType("table");
        schema.setOriginalName("id");
        schema.setComment("comment");
        schema.setName("id");
        schema.setTableAttr(new HashMap<>());
        final TapTable result = PdkSchemaConvert.toPdk(schema);
        Assertions.assertEquals(schema.getTableAttr(),result.getTableAttr());
    }

    @Test
    void testToPdk1_tableAttrValue() {
        // Setup
        final MetadataInstancesDto schema = new MetadataInstancesDto();
        schema.setMetaType("table");
        schema.setOriginalName("id");
        schema.setComment("comment");
        schema.setName("id");
        Map<String,Object> tableAttr = new HashMap<>();
        tableAttr.put("timeField","test");
        tableAttr.put("granularity","hours");
        schema.setTableAttr(tableAttr);
        final TapTable result = PdkSchemaConvert.toPdk(schema);
        Assertions.assertEquals(schema.getTableAttr(),result.getTableAttr());
    }

    @Test
    void testToPdk2_tableAttrValueNull() {
        // Setup
        final Schema schema = new Schema();
        schema.setMetaType("table");
        schema.setOriginalName("id");
        schema.setComment("comment");
        schema.setName("id");
        schema.setTableAttr(new HashMap<>());
        final TapTable result = PdkSchemaConvert.toPdk(schema);
        Assertions.assertEquals(schema.getTableAttr(),result.getTableAttr());
    }

    @Test
    void testToPdk2_tableAttrValue() {
        // Setup
        final Schema schema = new Schema();
        schema.setMetaType("table");
        schema.setOriginalName("id");
        schema.setComment("comment");
        schema.setName("id");
        Map<String,Object> tableAttr = new HashMap<>();
        tableAttr.put("timeField","test");
        tableAttr.put("granularity","hours");
        schema.setTableAttr(tableAttr);
        final TapTable result = PdkSchemaConvert.toPdk(schema);
        Assertions.assertEquals(schema.getTableAttr(),result.getTableAttr());
    }

    @Test
    void testFromPdk_tableAttrValue() {
        final TapTable tapTable = new TapTable("id", "id");
        Map<String,Object> tableAttr = new HashMap<>();
        tableAttr.put("timeField","test");
        tableAttr.put("granularity","hours");
        tapTable.setTableAttr(tableAttr);
        final MetadataInstancesDto result = PdkSchemaConvert.fromPdk(tapTable);
        Assertions.assertEquals(tapTable.getTableAttr(),result.getTableAttr());
    }

    @Test
    void testFromPdk_tableAttrValueNull() {
        final TapTable tapTable = new TapTable("id", "id");
        tapTable.setTableAttr(new HashMap<>());
        final MetadataInstancesDto result = PdkSchemaConvert.fromPdk(tapTable);
        Assertions.assertEquals(tapTable.getTableAttr(),result.getTableAttr());
    }
    @Test
    void testFromPdkSchema_tableAttrValue() {
        final TapTable tapTable = new TapTable("id", "id");
        Map<String,Object> tableAttr = new HashMap<>();
        tableAttr.put("timeField","test");
        tableAttr.put("granularity","hours");
        tapTable.setTableAttr(tableAttr);
        final Schema result = PdkSchemaConvert.fromPdkSchema(tapTable);
        Assertions.assertEquals(tapTable.getTableAttr(),result.getTableAttr());
    }
    @Test
    void testFromPdkSchema_tableAttrValueNull() {
        final TapTable tapTable = new TapTable("id", "id");
        tapTable.setTableAttr(new HashMap<>());
        final Schema result = PdkSchemaConvert.fromPdkSchema(tapTable);
        Assertions.assertEquals(tapTable.getTableAttr(),result.getTableAttr());
    }

    @Nested
    class SetTapTypeToPdkTest {
        TapPartition partitionInfo;
        TapTable tapTable;
        List<TapPartitionField> partitionFields;
        TapPartitionField field1;
        TapPartitionField field2;
        TapPartitionField field3;
        LinkedHashMap<String, TapField> nameFieldMap;
        TapField tapField1;
        TapField tapField2;
        TapField tapField3;
        @Test
        void testNormal() {
            partitionInfo = mock(TapPartition.class);
            tapTable = mock(TapTable.class);
            partitionFields = new ArrayList<>();
            nameFieldMap = new LinkedHashMap<>();

            field1 = mock(TapPartitionField.class);
            when(field1.getName()).thenReturn("k1");
            doNothing().when(field1).setTapType(any(TapType.class));

            field2 = mock(TapPartitionField.class);
            when(field2.getName()).thenReturn("k2");
            doNothing().when(field2).setTapType(any(TapType.class));

            field3 = mock(TapPartitionField.class);
            when(field3.getName()).thenReturn("k3");
            doNothing().when(field3).setTapType(any(TapType.class));

            partitionFields.add(field1);
            partitionFields.add(field2);
            partitionFields.add(field3);
            partitionFields.add(null);

            tapField1 = mock(TapField.class);
            tapField2 = mock(TapField.class);
            tapField3 = mock(TapField.class);
            nameFieldMap.put("k1", tapField1);
            nameFieldMap.put("k2", tapField2);
            when(tapField1.getTapType()).thenReturn(TapSimplify.tapArray());
            when(tapField2.getTapType()).thenReturn(TapSimplify.tapArray());
            when(tapField3.getTapType()).thenReturn(TapSimplify.tapArray());


            when(partitionInfo.getPartitionFields()).thenReturn(partitionFields);
            when(tapTable.getNameFieldMap()).thenReturn(nameFieldMap);

            Assertions.assertDoesNotThrow(() -> PdkSchemaConvert.setTapTypeToPdk(partitionInfo, tapTable));
        }
    }
}
