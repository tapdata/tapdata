package com.tapdata.tm.commons.dag.process.converter;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TmSchemaConverterTest {

    @Test
    void convertReturnsEmptyListForNullOrEmptyInput() {
        TmSchemaConverter converter = new TmSchemaConverter();

        assertNotNull(converter.convert(null));
        assertTrue(converter.convert(null).isEmpty());
        assertTrue(converter.convert(List.of()).isEmpty());
    }

    @Test
    void convertSkipsNullSchemaEntries() {
        TmSchemaConverter converter = new TmSchemaConverter();
        Schema schema = buildSchema("node_1", "orders");
        schema.setFields(List.of(buildField("id", true, 1, false, 1, "INT", "id")));

        List<TapTableDto> result = converter.convert(Arrays.asList(null, schema, null));

        assertEquals(1, result.size());
        assertEquals("node_1", result.get(0).getId());
        assertEquals("orders", result.get(0).getName());
    }

    @Test
    void convertSingleReturnsNullForNullSchema() {
        TmSchemaConverter converter = new TmSchemaConverter();
        assertNull(converter.convertSingle(null));
    }

    @Test
    void convertSingleMapsSchemaFieldsPrimaryKeysAndIndex() {
        TmSchemaConverter converter = new TmSchemaConverter();
        Schema schema = buildSchema("node_1", "orders");

        Field idField = buildField("id", true, 1, false, 1, "INT", "id");
        Field amountField = buildField("amount", false, null, true, 2, "DOUBLE", "amount");
        schema.setFields(List.of(idField, amountField));
        schema.setIndices(List.of(buildIndex("idx_orders_id", "id")));

        TapTableDto dto = converter.convertSingle(schema);

        assertNotNull(dto);
        assertEquals("node_1", dto.getId());
        assertEquals("orders", dto.getName());
        assertEquals(List.of("id"), dto.getPrimaryKeys());
        assertNotNull(dto.getFields());
        assertEquals(2, dto.getFields().size());

        TapFieldDto idDto = dto.getFields().get(0);
        assertEquals("id", idDto.getName());
        assertEquals("id", idDto.getOriginalFieldName());
        assertEquals("INT", idDto.getDataType());
        assertTrue(idDto.getIsPrimaryKey());
        assertEquals(1, idDto.getPrimaryKeyPos());
        assertFalse(idDto.getNullable());
        assertEquals(1, idDto.getPos());
        assertEquals("Int", idDto.getArrowTypeName());
        assertEquals(32, idDto.getArrowBitWidth());
        assertEquals("INTEGER", idDto.getDuckDbTypeName());

        TapFieldDto amountDto = dto.getFields().get(1);
        assertEquals("amount", amountDto.getName());
        assertFalse(amountDto.getIsPrimaryKey());
        assertNull(amountDto.getPrimaryKeyPos());
        assertTrue(amountDto.getNullable());
        assertEquals(2, amountDto.getPos());
        assertEquals("FloatingPoint", amountDto.getArrowTypeName());
        assertEquals("DOUBLE", amountDto.getArrowPrecision());
        assertEquals("DOUBLE", amountDto.getDuckDbTypeName());

        assertNotNull(dto.getIndexes());
        assertEquals("idx_orders_id", dto.getIndexes().getIndexName());
        assertEquals("id", dto.getIndexes().getColumns().get(0).getColumnName());
    }

    @Test
    void convertSingleFallsBackToSchemaIdWhenNodeIdMissing() {
        TmSchemaConverter converter = new TmSchemaConverter();
        Schema schema = buildSchema(null, "customers");
        ObjectId objectId = new ObjectId();
        schema.setId(objectId);

        TapTableDto dto = converter.convertSingle(schema);

        assertEquals(objectId.toString(), dto.getId());
        assertEquals("customers", dto.getName());
    }

    @Test
    void convertSingleUsesDefaultNullableAndSkipsNonPositivePositions() {
        TmSchemaConverter converter = new TmSchemaConverter();
        Schema schema = buildSchema("node_2", "inventory");

        Field field = new Field();
        field.setFieldName("sku");
        field.setOriginalFieldName("sku");
        field.setDataType("VARCHAR");
        field.setPrimaryKey(false);
        field.setPrimaryKeyPosition(0);
        field.setIsNullable("false");
        field.setColumnPosition(0);
        schema.setFields(List.of(field));

        TapTableDto dto = converter.convertSingle(schema);
        TapFieldDto fieldDto = dto.getFields().get(0);

        assertTrue(fieldDto.getNullable());
        assertNull(fieldDto.getPrimaryKeyPos());
        assertNull(fieldDto.getPos());
        assertEquals("Utf8", fieldDto.getArrowTypeName());
        assertEquals("VARCHAR", fieldDto.getDuckDbTypeName());
    }

    @Test
    void convertSingleDoesNotReuseInstanceForRepeatedConversionBecauseCacheIsDisabled() {
        TmSchemaConverter converter = new TmSchemaConverter();
        Schema schema = buildSchema("node_3", "payments");
        schema.setFields(List.of(buildField("id", true, 1, false, 1, "BIGINT", "id")));

        TapTableDto first = converter.convertSingle(schema);
        TapTableDto second = converter.convertSingle(schema);

        assertNotSame(first, second);
        assertEquals(first, second);
        assertNotSame(first.getFields().get(0), second.getFields().get(0));
    }

    @Test
    void clearCacheCanBeCalledSafely() {
        TmSchemaConverter converter = new TmSchemaConverter();
        converter.setCacheExpireTime(1L);
        converter.setCleanupInterval(1L);

        assertDoesNotThrow(converter::clearCache);
    }

    private static Schema buildSchema(String nodeId, String name) {
        Schema schema = new Schema();
        schema.setNodeId(nodeId);
        schema.setName(name);
        return schema;
    }

    private static Field buildField(String name, boolean primaryKey, Integer primaryKeyPos,
                                    Object nullable, Integer columnPosition,
                                    String dataType, String originalFieldName) {
        Field field = new Field();
        field.setFieldName(name);
        field.setPrimaryKey(primaryKey);
        field.setPrimaryKeyPosition(primaryKeyPos);
        field.setIsNullable(nullable);
        field.setColumnPosition(columnPosition);
        field.setDataType(dataType);
        field.setOriginalFieldName(originalFieldName);
        return field;
    }

    private static TableIndex buildIndex(String name, String columnName) {
        TableIndex index = new TableIndex();
        index.setIndexName(name);
        TableIndexColumn column = new TableIndexColumn();
        column.setColumnName(columnName);
        column.setColumnPosition(1);
        List<TableIndexColumn> columns = new ArrayList<>();
        columns.add(column);
        index.setColumns(columns);
        return index;
    }
}
