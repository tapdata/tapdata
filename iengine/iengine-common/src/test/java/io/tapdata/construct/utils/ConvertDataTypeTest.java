package io.tapdata.construct.utils;

import com.tapdata.constant.DateUtil;
import io.tapdata.entity.schema.value.DateTime;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class ConvertDataTypeTest {
    /**
     * 测试Null值
     * expected null
     */
    @Test
    void testNullValue() {
        Object o = DataUtil.convertDataType(null);
        assertNull(o);
    }

    /**
     * 测试不是转化类型的Value
     */
    @Test
    void testNotConversionTypeValue() {
        String str = "Hello World!";
        Object o = DataUtil.convertDataType(str);
        assertTrue(o instanceof String);
        assertEquals(str, o.toString());
    }

    /**
     * 测试Instant类型转为Date类型
     */
    @Test
    void testInstantValue() {
        Instant instant = Instant.ofEpochSecond(1631714400);
        Object o = DataUtil.convertDataType(instant);
        assertTrue(o instanceof Date);
    }

    /**
     * 测试Instant的数值超过Date类型的范围的转化
     */
    @Test
    void testInstantMax() {
        Instant max = Instant.MAX;
        assertThrowsExactly(IllegalArgumentException.class, () -> DataUtil.convertDataType(max));
    }


    /**
     * 测试BigDecimal精度小于34位时转换为Decimal128
     */
    @Test
    void testBigDecimalPrecisionLessThan34() {
        BigDecimal bigDecimal = new BigDecimal("3.14");
        Object o = DataUtil.convertDataType(bigDecimal);
        Decimal128 decimal128 = new Decimal128(bigDecimal);
        assertTrue(o instanceof Decimal128);
        assertEquals(decimal128, o);
    }

    /**
     * 测试BigDecimal精度大于34位时转换为Decimal128
     */
    @Test
    void testBigDecimalPrecisionMoreThan34() {
        BigDecimal bigDecimal = new BigDecimal("3.1688888888888888888888888888888888888888888888");
        BigDecimal expectedBigDecimal = new BigDecimal("3.168888888888888888888888888888889");
        Object o = DataUtil.convertDataType(bigDecimal);
        assertTrue(o instanceof Decimal128);
        assertEquals(expectedBigDecimal, ((Decimal128) o).bigDecimalValue());
    }

    /**
     * 测试DateTime转为Date "DateTime nano 0 seconds 1002561759 timeZone null"
     */
    @Test
    void testDateTimeValue() {
        DateTime dateTime = new DateTime();
        dateTime.setNano(0);
        dateTime.setSeconds(1002561759L);
        Object o = DataUtil.convertDataType(dateTime);
        assertTrue(o instanceof Date);
    }

    /**
     * 测试DateTime 的 seconds 为null 的情况
     */
    @Test
    void testDateTimeSecondsIsNull() {
        DateTime dateTime = new DateTime();
        dateTime.setSeconds(null);
        dateTime.setNano(0);
        Object result = DataUtil.convertDataType(dateTime);
        assertTrue(null == result);
    }

    /**
     * 测试超过long范围的值转为String
     */
    @Test
    void testBigIntegerValue() {
        BigInteger bigInteger = new BigInteger("9223372036854775807");
        Object o = DataUtil.convertDataType(bigInteger);
        assertTrue(o instanceof String);
        assertEquals("9223372036854775807", o.toString());
    }
}
