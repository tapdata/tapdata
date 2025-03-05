package io.tapdata.huawei.drs.kafka.types;

import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/26 16:57 Create
 */
class BytesTypeTest extends AbsTypeTest<BytesType, TapBinaryValue> {
    public BytesTypeTest() {
        super(TapBinaryValue.class, new TapStringValue("中文"), BasicType.bytesStringEncode("中文".getBytes()), "中文".getBytes(), 1);
    }

    @Override
    protected BytesType init() {
        return new BytesType("bytes");
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertInstanceOf(TapBinaryValue.class, result);
        Assertions.assertEquals(mockBytes, ((TapBinaryValue) result).getValue());
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapBinaryValue.class, result);
        Assertions.assertEquals("中文", new String(((TapBinaryValue) result).getValue()));
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertInstanceOf(TapBinaryValue.class, result);
        Assertions.assertEquals(mockInteger.toString(), new String(((TapBinaryValue) result).getValue()));
    }
}
