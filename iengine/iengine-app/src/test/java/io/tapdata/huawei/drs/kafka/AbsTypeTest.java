package io.tapdata.huawei.drs.kafka;

import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.huawei.drs.kafka.types.BasicType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/26 16:51 Create
 */
public abstract class AbsTypeTest<T extends BasicType, P extends TapValue> {
    protected T type;
    protected Class<P> valueType;
    protected TapValue<?, ?> mockTapValue;
    protected String mockString;
    protected byte[] mockBytes;
    protected Integer mockInteger;

    protected AbsTypeTest(Class<P> valueType, TapValue<?, ?> mockTapValue, String mockString, byte[] mockBytes, Integer mockInteger) {
        this.valueType = valueType;
        this.mockTapValue = mockTapValue;
        this.mockString = mockString;
        this.mockBytes = mockBytes;
        this.mockInteger = mockInteger;
    }

    protected abstract T init();

    @BeforeEach
    protected void setUp() {
        type = init();
    }

    @Test
    protected void testDecodeNull() {
        Object result = type.decode(null);
        Assertions.assertInstanceOf(valueType, result);
        Assertions.assertNull(((TapValue) result).getValue());
    }

    @Test
    protected void testDecodeTapValue() {
        Object result = type.decode(mockTapValue);
        Assertions.assertEquals(mockTapValue, result);
    }

    @Test
    protected void testDecodeBytes() {
        Object result = type.decode(mockBytes);
        assertDecodeBytes(result);
    }

    protected abstract void assertDecodeBytes(Object result);

    @Test
    protected void testDecodeString() {
        Object result = type.decode(mockString);
        assertDecodeString(result);
    }

    protected abstract void assertDecodeString(Object result);

    @Test
    protected void testDecodeInteger() {
        Object result = type.decode(mockInteger);
        assertDecodeInteger(result);
    }

    protected abstract void assertDecodeInteger(Object result);
}
