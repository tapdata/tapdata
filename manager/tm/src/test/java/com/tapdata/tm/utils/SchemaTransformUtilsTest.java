package com.tapdata.tm.utils;

import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.FileProperty;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.bean.*;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaTransformUtilsTest {
    @Test
    void testOldSchema2newSchema_tableAttrValue() {
        // Setup
        final Table table = new Table();
        table.setTableName("id");
        table.setMetaType("metaType");
        table.setTableId("tableId");
        Map<String,Object> tableAttr = new HashMap<>();
        tableAttr.put("timeField","test");
        tableAttr.put("granularity","hours");
        table.setTableAttr(tableAttr);
        final Schema schema = new Schema(Arrays.asList(table));
        final List<MetadataInstancesDto> result = SchemaTransformUtils.oldSchema2newSchema(schema);
        Assertions.assertEquals(schema.getTables().get(0).getTableAttr(),result.get(0).getTableAttr());
    }

    @Test
    void testOldSchema2newSchema_tableAttrValueNull() {
        // Setup
        final Table table = new Table();
        table.setTableName("id");
        table.setMetaType("metaType");
        table.setTableId("tableId");
        table.setTableAttr(new HashMap<>());
        final Schema schema = new Schema(Arrays.asList(table));
        final List<MetadataInstancesDto> result = SchemaTransformUtils.oldSchema2newSchema(schema);
        Assertions.assertEquals(schema.getTables().get(0).getTableAttr(),result.get(0).getTableAttr());
    }

    @Test
    void testNewSchema2oldSchema_tableAttrValue() {
        final MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
        metadataInstancesDto.setId(new ObjectId(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(), 0));
        metadataInstancesDto.setMetaType("metaType");
        metadataInstancesDto.setOriginalName("id");
        metadataInstancesDto.setSchemaVersion("schemaVersion");
        Map<String,Object> tableAttr = new HashMap<>();
        tableAttr.put("timeField","test");
        tableAttr.put("granularity","hours");
        metadataInstancesDto.setTableAttr(tableAttr);
        final List<MetadataInstancesDto> tables = Arrays.asList(metadataInstancesDto);
        final Schema result = SchemaTransformUtils.newSchema2oldSchema(tables);
        Assertions.assertEquals(tables.get(0).getTableAttr(),result.getTables().get(0).getTableAttr());
    }

    @Test
    void testNewSchema2oldSchema_tableAttrValueNull() {
        final MetadataInstancesDto metadataInstancesDto = new MetadataInstancesDto();
        metadataInstancesDto.setId(new ObjectId(new GregorianCalendar(2020, Calendar.JANUARY, 1).getTime(), 0));
        metadataInstancesDto.setMetaType("metaType");
        metadataInstancesDto.setOriginalName("id");
        metadataInstancesDto.setSchemaVersion("schemaVersion");
        metadataInstancesDto.setTableAttr(new HashMap<>());
        final List<MetadataInstancesDto> tables = Arrays.asList(metadataInstancesDto);
        final Schema result = SchemaTransformUtils.newSchema2oldSchema(tables);
        Assertions.assertEquals(tables.get(0).getTableAttr(),result.getTables().get(0).getTableAttr());
    }
    @Test
    void testGetRelation_tableAttrValue() {
        final Table table = new Table();
        table.setTableName("id");
        table.setMetaType("metaType");
        table.setTableId("tableId");
        Map<String,Object> tableAttr = new HashMap<>();
        tableAttr.put("timeField","test");
        tableAttr.put("granularity","hours");
        table.setTableAttr(tableAttr);
        final List<Table> tables = Arrays.asList(table);
        final Relation relation = new Relation();
        Field oldField = new Field();
        oldField.setForeignKeyTable("test");
        oldField.setForeignKeyColumn("test");
        final Pair<Integer, Relation> result = SchemaTransformUtils.getRelation(relation, oldField, tables);
        Assertions.assertEquals(tables.get(0).getTableAttr(),result.getValue().getForeignKeyTable().getTableAttr());
    }
    @Test
    void testGetRelation_tableAttrValueNull() {
        final Table table = new Table();
        table.setTableName("id");
        table.setMetaType("metaType");
        table.setTableId("tableId");
        table.setTableAttr(new HashMap<>());
        final List<Table> tables = Arrays.asList(table);
        final Relation relation = new Relation();
        Field oldField = new Field();
        oldField.setForeignKeyTable("test");
        oldField.setForeignKeyColumn("test");
        final Pair<Integer, Relation> result = SchemaTransformUtils.getRelation(relation, oldField, tables);
        Assertions.assertEquals(tables.get(0).getTableAttr(),result.getValue().getForeignKeyTable().getTableAttr());
    }
}
