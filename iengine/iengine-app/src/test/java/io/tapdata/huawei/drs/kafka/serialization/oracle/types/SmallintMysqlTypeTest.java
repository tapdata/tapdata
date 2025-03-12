package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/26 18:41 Create
 */
public class SmallintMysqlTypeTest extends AbsTypeTest<SmallintMysqlType, TapNumberValue> {
    public SmallintMysqlTypeTest() {
        super(TapNumberValue.class, new TapStringValue("中文"), "1", "1".getBytes(), 1);
    }

    @Override
    protected SmallintMysqlType init() {
        return new SmallintMysqlType();
    }

    @Test
    @Override
    protected void testDecodeNull() {
        Object result = type.decode(null);
        Assertions.assertInstanceOf(TapNumberValue.class, result);
        Assertions.assertNull(((TapNumberValue) result).getValue());
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertInstanceOf(TapNumberValue.class, result);
        Assertions.assertEquals(Double.parseDouble(new String(mockBytes)), ((TapNumberValue) result).getValue());
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapNumberValue.class, result);
        Assertions.assertEquals(Double.parseDouble(mockString), ((TapNumberValue) result).getValue());
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertInstanceOf(TapNumberValue.class, result);
        Assertions.assertEquals(mockInteger.doubleValue(), ((TapNumberValue) result).getValue());
    }

    @Test
    void testDecodeError() {
        Map<String, Object> mockValue = new HashMap<>();
        Assertions.assertThrows(RuntimeException.class, () -> type.decode(mockValue));
    }
}
