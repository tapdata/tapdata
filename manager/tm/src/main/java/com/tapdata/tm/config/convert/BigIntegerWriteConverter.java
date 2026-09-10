package com.tapdata.tm.config.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

import java.math.BigInteger;

@WritingConverter
public class BigIntegerWriteConverter implements Converter<BigInteger, String> {
    @Override
    public String convert(BigInteger source) {
        return source.toString();
    }
}
