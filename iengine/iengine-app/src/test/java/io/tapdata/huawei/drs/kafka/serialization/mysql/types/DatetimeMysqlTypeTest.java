package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 14:12 Create
 */
class DatetimeMysqlTypeTest extends AbsTypeTest<DatetimeMysqlType, TapDateTimeValue> {
    static final String TIME_STRING = "2025-02-27 00:00:00";

    public DatetimeMysqlTypeTest() {
        super(TapDateTimeValue.class, new TapStringValue("中文"), TIME_STRING, TIME_STRING.getBytes(), 1);
    }

    @Override
    protected DatetimeMysqlType init() {
        return new DatetimeMysqlType();
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertEquals(mockBytes, result);
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapDateTimeValue.class, result);
        Assertions.assertEquals(TIME_STRING, ((TapDateTimeValue) result).getValue().toFormatStringV2("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertEquals(mockInteger, result);
    }
}
