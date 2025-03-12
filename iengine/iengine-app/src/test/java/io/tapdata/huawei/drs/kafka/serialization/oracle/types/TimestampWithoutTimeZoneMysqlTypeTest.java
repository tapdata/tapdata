package io.tapdata.huawei.drs.kafka.serialization.oracle.types;

import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 15:24 Create
 */
class TimestampWithoutTimeZoneMysqlTypeTest extends AbsTypeTest<TimestampWithoutTimeZoneMysqlType, TapDateTimeValue> {
    static final String TIME_STRING = "2025-02-27 00:00:01.000001";

    public TimestampWithoutTimeZoneMysqlTypeTest() {
        super(TapDateTimeValue.class, new TapStringValue("中文"), TIME_STRING, TIME_STRING.getBytes(), 1);
    }

    @Override
    protected TimestampWithoutTimeZoneMysqlType init() {
        return new TimestampWithoutTimeZoneMysqlType();
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertEquals(mockBytes, result);
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapDateTimeValue.class, result);
        Assertions.assertEquals(TIME_STRING, ((TapDateTimeValue) result).getValue().toFormatStringV2("yyyy-MM-dd HH:mm:ss.SSSSSS"));
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertEquals(mockInteger, result);
    }
}
