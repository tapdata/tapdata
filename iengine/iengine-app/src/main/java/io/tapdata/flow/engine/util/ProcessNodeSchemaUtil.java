package io.tapdata.flow.engine.util;

import cn.hutool.core.collection.CollUtil;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.JavaTypesToTapTypes;
import io.tapdata.observable.logging.ObsLogger;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class ProcessNodeSchemaUtil {
    private ProcessNodeSchemaUtil() {
    }

    public static void scanTapField(TapTable tapTable, Map<String, TapField> oldNameFieldMap, String fieldName, Object value, ObsLogger obsLogger) {
        if (obsLogger.isDebugEnabled()) {
            obsLogger.debug("entry type: {} - {}", fieldName, null != value ? value.getClass() : "null");
        }
        TapType tapType;
        if (value instanceof TapValue) {
            TapValue<?, ?> tapValue = (TapValue<?, ?>) value;
            tapType = tapValue.getTapType();
        } else {
            tapType = JavaTypesToTapTypes.toTapType(value);
        }
        if (tapType == null) {
            tapType = TapSimplify.tapRaw();
        }
        scanSubFieldFromEventValue(tapTable, oldNameFieldMap, fieldName, value, tapType, obsLogger);
        TapField tapField = null;
        if (oldNameFieldMap != null) {
            TapField oldTapField = oldNameFieldMap.get(fieldName);
            if (oldTapField != null && oldTapField.getTapType() != null
                    && (oldTapField.getTapType().getType() == tapType.getType() || tapType.getType() == TapType.TYPE_RAW)) {
                tapField = oldTapField;
            }
        }
        if (tapField == null) {
            tapField = new TapField().name(fieldName).tapType(tapType);
        }
        tapTable.add(tapField);
    }

    protected static void scanSubFieldFromEventValue(TapTable tapTable, Map<String, TapField> oldNameFieldMap, String entryKey, Object entryValue, TapType tapType, ObsLogger obsLogger) {
        if (null == entryValue) {
            return;
        }
        switch (tapType.getType()) {
            case TapType.TYPE_MAP:
            case TapType.TYPE_ARRAY:
                scanObject(tapTable, oldNameFieldMap, entryKey, entryValue, obsLogger);
                break;
            default:
                //do nothing
        }
    }

    protected static void scanObject(TapTable tapTable, Map<String, TapField> oldNameFieldMap, String fatherFieldName, Object entryValue, ObsLogger obsLogger) {
        if (entryValue instanceof Map) {
            scanMap(tapTable, oldNameFieldMap, fatherFieldName, (Map<String, Object>) entryValue, obsLogger);
        } else if (entryValue instanceof TapMapValue) {
            scanMap(tapTable, oldNameFieldMap, fatherFieldName, ((TapMapValue) entryValue).getValue(), obsLogger);
        } else if (entryValue instanceof Collection) {
            scanArray(tapTable, oldNameFieldMap, fatherFieldName, (Collection<Object>) entryValue, obsLogger);
        } else if (entryValue instanceof TapArrayValue) {
            scanArray(tapTable, oldNameFieldMap, fatherFieldName, ((TapArrayValue) entryValue).getValue(), obsLogger);
        }
    }

    protected static void scanMap(TapTable tapTable, Map<String, TapField> oldNameFieldMap, String fatherFieldName, Map<String, Object> mapValue, ObsLogger obsLogger) {
        mapValue.forEach((key, value) -> {
            String currentFieldName = String.format("%s.%s", fatherFieldName, key);
            scanTapField(tapTable, oldNameFieldMap, currentFieldName, value, obsLogger);
        });
    }

    protected static void scanArray(TapTable tapTable, Map<String, TapField> oldNameFieldMap, String fatherFieldName, Collection<Object> arrayValue, ObsLogger obsLogger) {
        arrayValue.stream().filter(Objects::nonNull).forEach(arrayItem -> scanObject(tapTable, oldNameFieldMap, fatherFieldName, arrayItem, obsLogger));
    }

    public static void retainedOldSubFields(TapTable tapTable, Map<String, TapField> oldNameFieldMap, Map<String, Object> afterValue) {
        if (CollUtil.isEmpty(oldNameFieldMap)) {
            return;
        }
        oldNameFieldMap.entrySet().stream().filter(entry -> {
            String key = entry.getKey();
            int index = key.indexOf(".");
            if (index > 0) {
                String fatherFieldName = key.substring(0, index);
                return needRetainedOldSubField(tapTable, afterValue, oldNameFieldMap, key, fatherFieldName);
            }
            return false;
        }).forEach(e -> tapTable.add(e.getValue()));
    }

    private static boolean needRetainedOldSubField(TapTable tapTable,
                                                   Map<String, Object> afterValue,
                                                   Map<String, TapField> oldNameFieldMap,
                                                   String key, String fatherFieldName) {
        if (afterValue.containsKey(fatherFieldName) && !afterValue.containsKey(key)) {
            TapField oldFatherField = oldNameFieldMap.get(fatherFieldName);
            if (null == oldFatherField) {
                return false;
            }
            TapField afterField = tapTable.getNameFieldMap().get(fatherFieldName);
            if (null == afterField || null == afterField.getTapType()) {
                return false;
            }
            return afterField.getTapType().getType() == oldFatherField.getTapType().getType();
        }
        return false;
    }
}
