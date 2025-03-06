package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 11:08 Create
 */
class FloatMysqlTypeTest extends AbsTypeTest<FloatMysqlType, TapNumberValue> {
    public FloatMysqlTypeTest() {
        super(TapNumberValue.class, new TapStringValue("中文"), "1.1", "1.1".getBytes(), 1);
    }

    @Override
    protected FloatMysqlType init() {
        return new FloatMysqlType();
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
}
