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
    protected int maxScale = 6;
    protected int scale = 2;
    protected double scaleCompare = 0.01;
    protected RoundingMode roundingMode = RoundingMode.HALF_UP;
    protected boolean boundEnabled = false;
    protected double boundMin = 0D;
    protected double boundMax = 0D;

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException {
        if (value == null) {
            gen.writeNull();
            return;
        }
        if (value instanceof Double dVal) {
            Double normalized = normalize(dVal);
            if (normalized == null) {
                gen.writeNull();
            } else {
                gen.writeNumber(format(normalized));
            }
        } else if (value instanceof List<?> lVal) {
            try {
                gen.writeStartArray();
                for (Object val : lVal) {
                    if (null == val) {
                        gen.writeNull();
                    } else if (val instanceof Double dVal) {
                        Double normalized = normalize(dVal);
                        if (normalized == null) {
                            gen.writeNull();
                        } else {
                            gen.writeNumber(format(normalized));
                        }
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
                serializer.maxScale = format.maxScale();
                serializer.roundingMode = format.roundingMode();
                serializer.scaleCompare = BigDecimal.ONE.movePointLeft(serializer.scale).doubleValue();
                DoubleValueBound bound = property.getAnnotation(DoubleValueBound.class);
                if (bound != null) {
                    serializer.boundEnabled = true;
                    serializer.boundMin = bound.min();
                    serializer.boundMax = bound.max();
                }
                return serializer;
            }
        }
        return this;
    }

    protected Double normalize(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        if (!boundEnabled) {
            return value;
        }
        double low = Math.min(boundMin, boundMax);
        double high = Math.max(boundMin, boundMax);
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

    protected BigDecimal format(double value) {
        value = Math.max(value, 0D);
        BigDecimal original = BigDecimal.valueOf(value);
        int targetScale = scale;
        if (value > 0D && scaleCompare > value) {
            for (int s = scale + 1; s <= maxScale; s++) {
                BigDecimal tmp = original.setScale(s, RoundingMode.HALF_UP);
                if (tmp.signum() > 0) {
                    targetScale = s;
                    break;
                }
            }
        }
        BigDecimal bd = original
                .setScale(targetScale, RoundingMode.HALF_UP)
                .stripTrailingZeros();
        if (bd.scale() <= 0) {
            bd = bd.setScale(scale, RoundingMode.UNNECESSARY);
        }
        return bd;
    }
}
