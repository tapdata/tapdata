package com.tapdata.tm.config.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

import java.math.BigInteger;

/**
 * Symmetric counterpart of {@link BigIntegerWriteConverter}.
 * Only applies when a field is explicitly typed as {@link BigInteger}; fields typed as
 * {@link Object} (e.g. Field#defaultValue) are left untouched and read back as {@link String}.
 */
@ReadingConverter
public class BigIntegerReadConverter implements Converter<String, BigInteger> {
    @Override
    public BigInteger convert(String source) {
        return new BigInteger(source);
    }
}
