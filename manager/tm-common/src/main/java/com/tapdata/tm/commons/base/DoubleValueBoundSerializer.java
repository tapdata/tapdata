package com.tapdata.tm.commons.base;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;

import java.io.IOException;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/25 14:42 Create
 * @description
 */
public class DoubleValueBoundSerializer extends JsonSerializer<Object>
        implements ContextualSerializer {

    protected double min = 0D;
    protected double max = 100D;

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }

        if (value instanceof Number numberValue) {
            Double normalized = normalize(numberValue.doubleValue());
            if (normalized == null) {
                gen.writeNull();
            } else {
                gen.writeNumber(normalized);
            }
            return;
        }

        if (value instanceof List<?> listValue) {
            try {
                gen.writeStartArray();
                for (Object item : listValue) {
                    if (item == null) {
                        gen.writeNull();
                    } else if (item instanceof Number numberItem) {
                        Double normalized = normalize(numberItem.doubleValue());
                        if (normalized == null) {
                            gen.writeNull();
                        } else {
                            gen.writeNumber(normalized);
                        }
                    } else {
                        gen.writeObject(item);
                    }
                }
            } finally {
                gen.writeEndArray();
            }
            return;
        }

        gen.writeObject(value);
    }

    @Override
    public JsonSerializer<?> createContextual(SerializerProvider prov, BeanProperty property) throws JsonMappingException {
        if (property != null) {
            DoubleValueBound bound = property.getAnnotation(DoubleValueBound.class);
            if (bound != null) {
                DoubleValueBoundSerializer serializer = new DoubleValueBoundSerializer();
                serializer.min = bound.min();
                serializer.max = bound.max();
                return serializer;
            }
        }
        return this;
    }

    protected Double normalize(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        double low = Math.min(min, max);
        double high = Math.max(min, max);
        if (value < low) {
            return low;
        }
        if (value > high) {
            return high;
        }
        if (value == 0D) {
            return 0D;
        }
        return value;
    }
}
