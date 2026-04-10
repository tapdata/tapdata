package com.tapdata.constant;

import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.ClassHandlersV2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

public class ClassHandlersV2ToStringUtils {
    private static final ClassHandlersV2 valueHandler = new ClassHandlersV2();
    static {
        valueHandler.register(Date.class, value -> value.toInstant().toString());
        valueHandler.register(LocalDateTime.class, LocalDateTime::toString);
        valueHandler.register(Instant.class, Instant::toString);
        valueHandler.register(TapDateTimeValue.class, tapValue -> tapValue.getValue().toInstant().toString());
        valueHandler.register(TapArrayValue.class, TapValue::getValue);
        valueHandler.register(TapMapValue.class, TapValue::getValue);
        valueHandler.register(TapStringValue.class, TapValue::getValue);
        valueHandler.register(TapRawValue.class, TapValue::getValue);
    }

    private static Object recursiveHandleValue(Object value) {
        Object newValue = valueHandler.handle(value);
        Object result = newValue != null ? newValue : value;
        if (result instanceof Map) {
            recursiveHandleMap((Map<String, Object>) result);
        } else if (result instanceof Collection) {
            recursiveHandleCollection((Collection<Object>) result);
        }
        return newValue;
    }

    public static void recursiveHandleMap(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object newValue = recursiveHandleValue(entry.getValue());
            if (newValue != null) {
                entry.setValue(newValue);
            }
        }
    }

    private static void recursiveHandleCollection(Collection<Object> collection) {
        List<Object> original = new ArrayList<>(collection);
        collection.clear();
        for (Object item : original) {
            Object newItem = recursiveHandleValue(item);
            collection.add(newItem != null ? newItem : item);
        }
    }


}
