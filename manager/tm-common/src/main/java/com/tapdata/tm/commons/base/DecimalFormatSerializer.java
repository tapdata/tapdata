package com.tapdata.tm.commons.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class DecimalFormatSerializer extends JsonSerializer<Object>
        implements ContextualSerializer {
    protected int scale = 2;
    protected RoundingMode roundingMode = RoundingMode.HALF_UP;

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        if (value instanceof Double dVal) {
            BigDecimal decimal = BigDecimal.valueOf(dVal)
                    .setScale(scale, roundingMode);
            gen.writeNumber(decimal);
        } else if (value instanceof List<?> lVal) {
            try {
                gen.writeStartArray();
                for (Object val : lVal) {
                    if (null == val) {
                        gen.writeNull();
                    } else if (val instanceof Double dVal) {
                        BigDecimal decimal = BigDecimal.valueOf(dVal)
                                .setScale(scale, roundingMode);
                        gen.writeNumber(decimal);
                    } else {
                        gen.writeObject(val);
                    }
                }
            } finally {
                gen.writeEndArray();
            }
        } else {
            gen.writeObject(value);
        }
    }

    @Override
    public JsonSerializer<?> createContextual(
            SerializerProvider prov,
            BeanProperty property
    ) throws JsonMappingException {
        if (property != null) {
            DecimalFormat format = property.getAnnotation(DecimalFormat.class);
            if (format != null) {
                DecimalFormatSerializer serializer = new DecimalFormatSerializer();
                serializer.scale = format.scale();
                serializer.roundingMode = format.roundingMode();
                return serializer;
            }
        }
        return this;
    }
}
