package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.schema.*;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

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

}
