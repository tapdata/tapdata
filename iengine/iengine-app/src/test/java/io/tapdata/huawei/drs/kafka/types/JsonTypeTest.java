package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/26 16:57 Create
 */
class JsonTypeTest extends AbsTypeTest<JsonType, TapStringValue> {
    public JsonTypeTest() {
        super(TapStringValue.class, new TapStringValue("中文"), "{'id':1}", "{'id':1}".getBytes(), 1);
    }

    @Override
    protected JsonType init() {
        return new JsonType();
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
