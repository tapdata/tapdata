package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.schema.*;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
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
        List<TableIndex> tapIndexList = new ArrayList<>();
        TableIndex tableIndex = new TableIndex();
        List<TableIndexColumn> columns = new ArrayList<>();
        TableIndexColumn tableIndexColumn = new TableIndexColumn();
        tableIndexColumn.setColumnName("col1");
        tableIndexColumn.setColumnIsAsc(true);
        tableIndexColumn.setColumnSubPart(100);
        columns.add(tableIndexColumn);
        tableIndex.setColumns(columns);
        tapIndexList.add(tableIndex);
        schema.setIndices(tapIndexList);
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
        List<TableIndex> tapIndexList = new ArrayList<>();
        TableIndex tableIndex = new TableIndex();
        List<TableIndexColumn> columns = new ArrayList<>();
        TableIndexColumn tableIndexColumn = new TableIndexColumn();
        tableIndexColumn.setColumnName("col1");
        tableIndexColumn.setColumnIsAsc(true);
        tableIndexColumn.setColumnSubPart(null);
        columns.add(tableIndexColumn);
        tableIndex.setColumns(columns);
        tapIndexList.add(tableIndex);
        schema.setIndices(tapIndexList);
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
        List<TapIndex> indexList = new ArrayList<>();
        TapIndex tapIndex = new TapIndex();
        tapIndex.setName("index1");
        tapIndex.setPrimary(true);
        List<TapIndexField> indexFields = new ArrayList<>();
        TapIndexField indexField = new TapIndexField();
        indexField.setName("col1");
        indexField.setFieldAsc(true);
        indexField.setSubPart(100);
        indexFields.add(indexField);
        tapIndex.setIndexFields(indexFields);
        indexList.add(tapIndex);
        tapTable.setIndexList(indexList);
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
        List<TapIndex> indexList = new ArrayList<>();
        TapIndex tapIndex = new TapIndex();
        tapIndex.setName("index1");
        tapIndex.setPrimary(true);
        List<TapIndexField> indexFields = new ArrayList<>();
        TapIndexField indexField = new TapIndexField();
        indexField.setName("col1");
        indexField.setFieldAsc(true);
        indexField.setSubPart(100);
        indexFields.add(indexField);
        tapIndex.setIndexFields(indexFields);
        indexList.add(tapIndex);
        tapTable.setIndexList(indexList);
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
