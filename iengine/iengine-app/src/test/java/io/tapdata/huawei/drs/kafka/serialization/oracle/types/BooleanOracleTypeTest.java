package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/26 19:25 Create
 */
public class BooleanOracleTypeTest extends AbsTypeTest<BooleanOracleType, TapBooleanValue> {
    public BooleanOracleTypeTest() {
        super(TapBooleanValue.class, new TapStringValue("中文"), "true", "1".getBytes(), 1);
    }

    protected BooleanOracleType init() {
        return new BooleanOracleType();
    }

    @Test
    @Override
    protected void testDecodeNull() {
        Object result = type.decode(null);
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertNull(((TapBooleanValue) result).getValue());
    }

    @Test
    protected void testDecodeTrue() {
        Object result = type.decode(true);
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertTrue(((TapBooleanValue) result).getValue());
    }

    @Test
    protected void testDecodeFalse() {
        Object result = type.decode(false);
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertFalse(((TapBooleanValue) result).getValue());
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertEquals(Boolean.parseBoolean(new String(mockBytes)), ((TapBooleanValue) result).getValue());
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertEquals(Boolean.parseBoolean(mockString), ((TapBooleanValue) result).getValue());
    }

    @Test
    protected void testDecodeInteger() {
        Object result = type.decode(1);
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertEquals(true, ((TapBooleanValue) result).getValue());

        result = type.decode(0);
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertEquals(false, ((TapBooleanValue) result).getValue());
    }

    @Test
    void testDecodeError() {
        Map<String, Object> mockValue = new HashMap<>();
        Object result = type.decode(mockValue);
        Assertions.assertInstanceOf(TapBooleanValue.class, result);
        Assertions.assertEquals(Boolean.parseBoolean(mockValue.toString()), ((TapBooleanValue) result).getValue());
    }
    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.fail("Not the expected call");
    }
}
