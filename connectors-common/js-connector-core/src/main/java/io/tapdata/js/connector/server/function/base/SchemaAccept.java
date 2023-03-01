package io.tapdata.js.connector.server.function.base;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.enums.JSTableKeys;

import java.util.*;
import java.util.function.Consumer;

public class SchemaAccept implements SchemaSender {
    private static final String TAG = SchemaAccept.class.getSimpleName();
    private Consumer<List<TapTable>> consumer;

    @Override
    public void setConsumer(Consumer<List<TapTable>> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void send(Object schemaObj) {
        if (Objects.isNull(schemaObj)) {
            TapLogger.info(TAG, "No table information was loaded after discoverSchema was executed.");
            return;
        }
        List<TapTable> tables = new ArrayList<>();
        Set<Map.Entry<String, Object>> discoverSchema = new HashSet<>();
        if (schemaObj instanceof Map) {
            discoverSchema = ((Map<String, Object>) schemaObj).entrySet();
        } else if (schemaObj instanceof Collection) {
            Collection<Object> tableCollection = (Collection<Object>) schemaObj;
            tableCollection.stream().filter(Objects::nonNull).forEach(tab -> this.covertTable(tables, tab));
        } else {
            String tableId = String.valueOf(schemaObj);
            tables.add(new TapTable(tableId, tableId));
        }
        if (!discoverSchema.isEmpty()) {
            discoverSchema.stream().filter(Objects::nonNull).forEach(entry -> this.covertTable(tables, entry.getValue()));
        }
        consumer.accept(tables);
    }

    private void covertTable(List<TapTable> tables, Object value) {
        if (value instanceof String) {
            String table = (String) value;
            TapTable tapTable = new TapTable(table, table);
            tapTable.setNameFieldMap(new LinkedHashMap<>());
            tables.add(tapTable);
        } else if (value instanceof Map) {
            Map<String, Object> tableMap = (Map<String, Object>) value;
            TapTable tapTable = new TapTable();
            Object tableIdObj = tableMap.get(JSTableKeys.TABLE_NAME);
            if (Objects.isNull(tableIdObj)) {
                TapLogger.warn(TAG, "The declared table has no table name. Please assign a value to the 'name' field of the table.");
                return;
            }
            String tableId = (String) tableIdObj;
            Object tableCommentObj = tableMap.get(JSTableKeys.TABLE_COMMENT);

            Object fieldsMapObj = tableMap.get(JSTableKeys.TABLE_FIELD);
            if (Objects.isNull(fieldsMapObj)) {
                TapLogger.warn(TAG, String.format("The declared table does not contain any field information. If necessary, please add field information to Table [%s].", tableId));
            } else {
                Map<String, Object> columnMap = (Map<String, Object>) fieldsMapObj;
                columnMap.entrySet().stream().filter(field ->
                        Objects.nonNull(field) && Objects.nonNull(field.getKey())
                ).forEach(column -> tapTable.add(this.field(column)));
            }
            tapTable.setId(tableId);
            tapTable.setName(tableId);
            tapTable.setComment(Objects.isNull(tableCommentObj) ? null : String.valueOf(tableCommentObj));
            tables.add(tapTable);
        } else if (value instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) value;
            collection.stream().filter(Objects::nonNull).forEach(table -> {
                String tableName = String.valueOf(table);
                tables.add(new TapTable(tableName, tableName));
            });
        }
    }

    private TapField field(Map.Entry<String, Object> column) {
        TapField field = new TapField();
        String fieldName = column.getKey();
        field.setName(fieldName);
        Object fieldInfoObj = column.getValue();
        if (Objects.nonNull(fieldInfoObj)) {
            Map<String, Object> fieldInfo = (Map<String, Object>) fieldInfoObj;
            Object fieldTypeObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_TYPE);
            Object fieldDefaultObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_DEFAULT_VALUE);
            Object fieldNullAbleObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_NULLABLE);
            Object fieldPrimaryKeyObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_PRIMARY_KEY);
            Object fieldPrimaryKeyPosObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_PRIMARY_POS_KEY);
            Object fieldAutoIncObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_AUTO_INC);
            Object fieldFieldCommentObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_COMMENT);
            field.setComment(Objects.isNull(fieldFieldCommentObj) ? null : String.valueOf(fieldFieldCommentObj));
            try {
                field.setAutoInc((Boolean) fieldAutoIncObj);
            } catch (Exception ignored) {
            }
            field.setDataType(Objects.isNull(fieldTypeObj) ? null : String.valueOf(fieldTypeObj));
            field.setDefaultValue(Objects.isNull(fieldDefaultObj) ? null : String.valueOf(fieldDefaultObj));
            try {
                field.setNullable((Boolean) fieldNullAbleObj);
            } catch (Exception ignored) {
            }
            try {
                field.setPrimaryKey((Boolean) fieldPrimaryKeyObj);
            } catch (Exception ignored) {
            }
            try{
                field.setPrimaryKeyPos(((Number) Optional.ofNullable(fieldPrimaryKeyPosObj).orElse(1)).intValue());
            }catch (Exception e){
                field.setPrimaryKeyPos(1);
            }
        }
        return field;
    }
}