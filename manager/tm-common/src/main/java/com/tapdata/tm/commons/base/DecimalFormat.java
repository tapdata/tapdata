package com.tapdata.tm.commons.base;

import com.fasterxml.jackson.annotation.JacksonAnnotationsInside;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.math.RoundingMode;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@JacksonAnnotationsInside
@JsonSerialize(using = DecimalFormatSerializer.class)
public @interface DecimalFormat {

    /** Reserved decimal places, default 2 digits */
    int scale() default 2;

    /** Reserved decimal places, default 4 digits */
    int maxScale() default 6;

    /** Rounding method, default rounding */
    RoundingMode roundingMode() default RoundingMode.HALF_UP;
}
