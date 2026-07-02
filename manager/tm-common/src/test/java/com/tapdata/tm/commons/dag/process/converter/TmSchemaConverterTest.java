package com.tapdata.tm.commons.dag.process.converter;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    void convertSingleUsesNullIdWhenNodeIdAndSchemaIdMissing() {
        TmSchemaConverter converter = new TmSchemaConverter();
        Schema schema = buildSchema(null, "anonymous");

        TapTableDto dto = converter.convertSingle(schema);

        assertNull(dto.getId());
        assertEquals("anonymous", dto.getName());
        assertTrue(dto.getPrimaryKeys().isEmpty());
        assertTrue(dto.getFields().isEmpty());
        assertNull(dto.getIndexes());
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
    void convertSingleSetsTapTypeNameWithoutPrecomputingWhenDataTypeMissing() throws Exception {
        TmSchemaConverter converter = new TmSchemaConverter();
        Schema schema = buildSchema("node_4", "events");
        Field field = new Field();
        field.setFieldName("payload");
        field.setOriginalFieldName("payload");
        field.setTapType("tap-string");
        schema.setFields(List.of(field));

        TapTableDto dto = converter.convertSingle(schema);

        assertEquals(1, dto.getFields().size());
        TapFieldDto fieldDto = dto.getFields().get(0);
        assertEquals("payload", fieldDto.getName());
        assertEquals("String", fieldDto.getTapTypeName());
        assertNull(fieldDto.getArrowTypeName());
        assertNull(fieldDto.getDuckDbTypeName());
        assertNull(invokeConvertField(converter, null));
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

    @Test
    void tryCleanupShouldSkipWhenIntervalNotReached() throws Exception {
        TmSchemaConverter converter = new TmSchemaConverter();
        converter.setCleanupInterval(Long.MAX_VALUE);
        putCacheEntry(converter, "k1", new TapTableDto("1", "orders"));

        invokeTryCleanup(converter);

        assertEquals(1, cache(converter).size());
    }

    @Test
    void tryCleanupShouldRemoveExpiredEntries() throws Exception {
        TmSchemaConverter converter = new TmSchemaConverter();
        converter.setCleanupInterval(0L);
        converter.setCacheExpireTime(-1L);
        setPrivateField(converter, "lastCleanupTime", 0L);
        putCacheEntry(converter, "k1", new TapTableDto("1", "orders"));

        invokeTryCleanup(converter);

        assertTrue(cache(converter).isEmpty());
    }

    @Test
    void cacheEntryAccessorsShouldWork() throws Exception {
        Object entry = newCacheEntry(new TapTableDto("1", "orders"));
        Method getValue = entry.getClass().getDeclaredMethod("getValue");
        Method touch = entry.getClass().getDeclaredMethod("touch");
        getValue.setAccessible(true);
        touch.setAccessible(true);

        assertEquals(new TapTableDto("1", "orders"), getValue.invoke(entry));
        assertDoesNotThrow(() -> touch.invoke(entry));
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

    private static void invokeTryCleanup(TmSchemaConverter converter) throws Exception {
        Method method = TmSchemaConverter.class.getDeclaredMethod("tryCleanup");
        method.setAccessible(true);
        method.invoke(converter);
    }

    private static TapFieldDto invokeConvertField(TmSchemaConverter converter, Field field) throws Exception {
        Method method = TmSchemaConverter.class.getDeclaredMethod("convertField", Field.class);
        method.setAccessible(true);
        return (TapFieldDto) method.invoke(converter, field);
    }

    private static void putCacheEntry(TmSchemaConverter converter, String key, TapTableDto value) throws Exception {
        cache(converter).put(key, newCacheEntry(value));
    }

    private static Object newCacheEntry(TapTableDto value) throws Exception {
        Class<?> cacheEntryClass = Class.forName(TmSchemaConverter.class.getName() + "$CacheEntry");
        Constructor<?> constructor = cacheEntryClass.getDeclaredConstructor(Object.class);
        constructor.setAccessible(true);
        return constructor.newInstance(value);
    }

    private static Map<String, Object> cache(TmSchemaConverter converter) throws Exception {
        java.lang.reflect.Field field = TmSchemaConverter.class.getDeclaredField("cache");
        field.setAccessible(true);
        return (Map<String, Object>) field.get(converter);
    }

    private static void setPrivateField(TmSchemaConverter converter, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = TmSchemaConverter.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(converter, value);
    }
}
