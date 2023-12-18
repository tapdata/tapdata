package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.schema.*;
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

}
