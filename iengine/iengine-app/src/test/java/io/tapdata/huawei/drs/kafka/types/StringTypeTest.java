package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/26 16:12 Create
 */
class StringTypeTest extends AbsTypeTest<StringType, TapStringValue> {
    public StringTypeTest() {
        super(TapStringValue.class, new TapStringValue("中文"), "中文", "中文".getBytes(), 1);
    }

    @Override
    protected StringType init() {
        return new StringType("string");
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertEquals(new String(mockBytes), ((TapStringValue) result).getValue());
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapStringValue.class, result);
        Assertions.assertEquals(mockString, ((TapStringValue) result).getValue());
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertInstanceOf(TapStringValue.class, result);
        Assertions.assertEquals(mockInteger.toString(), ((TapStringValue) result).getValue());
    }
}
