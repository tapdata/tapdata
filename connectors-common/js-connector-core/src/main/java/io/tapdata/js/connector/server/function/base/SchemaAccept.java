package io.tapdata.js.connector.server.function.base;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.js.connector.enums.JSTableKeys;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.tapNumber;
import static io.tapdata.base.ConnectorBase.toJson;

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

            Object numberConfigObj = fieldInfo.get(JSTableKeys.TABLE_FIELD_NUMBER);


            field.setComment(Objects.isNull(fieldFieldCommentObj) ? null : String.valueOf(fieldFieldCommentObj));

            field.setAutoInc(this.boolValue(fieldAutoIncObj, false,"autoInc"));
            field.setDataType(Objects.isNull(fieldTypeObj) ? null : String.valueOf(fieldTypeObj));
            field.setDefaultValue(Objects.isNull(fieldDefaultObj) ? null : String.valueOf(fieldDefaultObj));
            field.setNullable(this.boolValue(fieldNullAbleObj, false,"nullable"));
            field.setPrimaryKey(this.boolValue(fieldPrimaryKeyObj, false,"primaryKey"));

            Optional.ofNullable(numberConfigObj).ifPresent(number ->{
                if (!(number instanceof Map)) return;
                Map<String, Object> map = (Map<String, Object>) number;
                TapNumber tapNumber = tapNumber();
                tapNumber.fixed(this.boolValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_FIXED)).orElse(false), false,JSTableKeys.TABLE_FIELD_NUMBER_FIXED));
                tapNumber.precision(this.integerValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_PRECISEION)).orElse(0), 0, JSTableKeys.TABLE_FIELD_NUMBER_PRECISEION));
                tapNumber.scale(this.integerValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_SCALE)).orElse(0), 0, JSTableKeys.TABLE_FIELD_NUMBER_SCALE));
                tapNumber.zerofill(this.boolValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_ZEROFILL)).orElse(false), false,JSTableKeys.TABLE_FIELD_NUMBER_ZEROFILL));
                tapNumber.unsigned(this.boolValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_UNSINGLED)).orElse(false), false,JSTableKeys.TABLE_FIELD_NUMBER_UNSINGLED));
                tapNumber.bit(this.integerValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_BIT)).orElse(0), 0, JSTableKeys.TABLE_FIELD_NUMBER_BIT));
                tapNumber.maxValue(this.bigDecimalValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_MAX)).orElse(Integer.MAX_VALUE), new BigDecimal(Integer.MAX_VALUE), JSTableKeys.TABLE_FIELD_NUMBER_MAX));
                tapNumber.minValue(this.bigDecimalValue(Optional.ofNullable(map.get(JSTableKeys.TABLE_FIELD_NUMBER_MIN)).orElse(Integer.MIN_VALUE), new BigDecimal(Integer.MIN_VALUE), JSTableKeys.TABLE_FIELD_NUMBER_MIN));
                field.setTapType(tapNumber);
            });

            Integer primaryPos = null;
            if (fieldPrimaryKeyPosObj instanceof String) {
                try {
                    primaryPos = Integer.parseInt((String) fieldPrimaryKeyPosObj);
                } catch (Throwable throwable) {
                    TapLogger.warn(TAG, "Field primaryPos'type must be int, but it's string now and can't cast to int value, please ensure that.");
                }
            } else if (fieldPrimaryKeyPosObj instanceof Number) {
                primaryPos = ((Number) fieldPrimaryKeyPosObj).intValue();
            }
            if (primaryPos != null && primaryPos <= 0) {
                TapLogger.warn(TAG, "Field primaryPos'type must be lager than zero, but it's less or equals zero now, please ensure that.");
                primaryPos = null;
            }
            if (field.getPrimaryKey() && Objects.nonNull(primaryPos)) {
                field.setPrimaryKeyPos(primaryPos);
            }
        }
        return field;
    }

    private Boolean boolValue(Object obj, boolean defaultValue, String keyName) {
        boolean objBool = defaultValue;
        if (obj instanceof String) {
            try {
                objBool = Boolean.parseBoolean((String) obj);
            } catch (Throwable throwable) {
                TapLogger.warn(TAG, "Field " + keyName + "'type must be boolean, but it's string now and can't cast to boolean value, please ensure that.");
                return defaultValue;
            }
        } else if (obj instanceof Boolean) {
            objBool = (Boolean) obj;
        } else {
            return defaultValue;
        }
        return objBool;
    }

    private Integer integerValue(Object obj, Integer defaultValue, String keyName) {
        Integer objBool = defaultValue;
        if (obj instanceof String) {
            try {
                objBool = Integer.parseInt((String) obj);
            } catch (Throwable throwable) {
                TapLogger.warn(TAG, "Field " + keyName + "'type must be integer, but it's string now and can't cast to integer value, please ensure that.");
                return defaultValue;
            }
        } else if (obj instanceof Number) {
            objBool = ((Number) obj).intValue();
        } else {
            return defaultValue;
        }
        return objBool;
    }

    private BigDecimal bigDecimalValue(Object obj, BigDecimal defaultValue, String keyName) {
        BigDecimal objBool = defaultValue;
        if (obj instanceof Long) {
            try {
                objBool = BigDecimal.valueOf((long) obj);
            } catch (Throwable throwable) {
                TapLogger.warn(TAG, "Field " + keyName + "'type must be long, but it's string now and can't cast to long value, please ensure that.");
                return defaultValue;
            }
        } else if (obj instanceof Double) {
            try {
                objBool = BigDecimal.valueOf((double) obj);
            } catch (Throwable throwable) {
                TapLogger.warn(TAG, "Field " + keyName + "'type must be double, but it's string now and can't cast to double value, please ensure that.");
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
        return objBool;
    }

    public String tableMap(TapTable tapTable) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        Map<String, Map<String, Object>> fields = new HashMap<>();
        Collection<String> keys = tapTable.primaryKeys(true);
        nameFieldMap.forEach((key, field)->{
            Map<String, Object> fieldConfig = new HashMap<>();
            fieldConfig.put(JSTableKeys.TABLE_FIELD_TYPE, field.getDataType());
            fieldConfig.put(JSTableKeys.TABLE_FIELD_DEFAULT_VALUE, field.getDefaultValue());
            fieldConfig.put(JSTableKeys.TABLE_FIELD_NULLABLE, field.getNullable());
            fieldConfig.put(JSTableKeys.TABLE_FIELD_PRIMARY_KEY, field.getPrimaryKey());
            fieldConfig.put(JSTableKeys.TABLE_FIELD_PRIMARY_POS_KEY, field.getPrimaryKeyPos());
            fieldConfig.put(JSTableKeys.TABLE_FIELD_AUTO_INC, field.getAutoInc());
            fieldConfig.put(JSTableKeys.TABLE_FIELD_COMMENT, field.getComment());
            fields.put(key, fieldConfig);
        });
        Map<String,Object> tableMap = new HashMap<>();
        tableMap.put(JSTableKeys.TABLE_NAME, tapTable.getId());
        tableMap.put(JSTableKeys.TABLE_KEYS, keys);
        tableMap.put(JSTableKeys.TABLE_FIELD, fields);
        return toJson(tableMap);
    }
}