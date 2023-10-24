package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapStringValue;

import java.util.*;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_STRING_VALUE, buildNumber = 0)
public class ToTapStringCodec implements ToTapValueCodec<TapStringValue> {
    @Override
    public TapStringValue toTapValue(Object value, TapType typeFromSchema) {
        TapStringValue stringValue;
        Class<?> clazz = value.getClass();
        if(value instanceof String) {
            stringValue = new TapStringValue((String) value);
        } /*else if(CodecUtils.isPrimitiveOrWrapper(clazz)) {
            stringValue = new TapStringValue(value.toString());
        }*/ else if (Collection.class.isAssignableFrom(clazz)) {
            Collection<?> collection = (Collection<?>) value;
            stringValue = new TapStringValue(Arrays.toString(collection.toArray()));
        } else if (Map.class.isAssignableFrom(clazz)) {
            Map<?, ?> map = (Map<?, ?>) value;
            stringValue = new TapStringValue(Arrays.toString(map.entrySet().toArray()));
        } else {
            stringValue = new TapStringValue(value.toString());
        }
        return stringValue;
    }
}
