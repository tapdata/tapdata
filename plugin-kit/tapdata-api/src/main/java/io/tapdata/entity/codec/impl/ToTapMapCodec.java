package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;

import java.util.Map;

@Implementation(value = ToTapValueCodec.class, type = TapDefaultCodecs.TAP_MAP_VALUE, buildNumber = 0)
public class ToTapMapCodec implements ToTapValueCodec<TapMapValue> {
    @Override
    public TapMapValue toTapValue(Object value, TapType typeFromSchema) {

        if(value instanceof Map) {
            return new TapMapValue((Map<String, Object>) value);
        } else if(value instanceof String) {
            try {
                return new TapMapValue(InstanceFactory.instance(JsonParser.class).fromJsonObject((String) value));
            } catch(Throwable throwable) {
                throwable.printStackTrace();
            }
        }

        return null;
    }
}
