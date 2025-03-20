package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 14:12 Create
 */
class TimestampMysqlTypeTest extends AbsTypeTest<TimestampMysqlType, TapDateTimeValue> {
    static final long TIME_LONG = 1740637308413L;
    static final int TIME_INT = (int) (TIME_LONG / 1000);
    static final String TIME_STRING = "1740637308.413";

    public TimestampMysqlTypeTest() {
        super(TapDateTimeValue.class, new TapStringValue("中文"), TIME_STRING, TIME_STRING.getBytes(), TIME_INT);
    }

    @Override
    protected TimestampMysqlType init() {
        return new TimestampMysqlType();
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertEquals(mockBytes, result);
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapDateTimeValue.class, result);
        Assertions.assertEquals(TIME_LONG, ((TapDateTimeValue) result).getValue().toEpochMilli());
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertInstanceOf(TapDateTimeValue.class, result);
        Assertions.assertEquals(1000L * TIME_INT, ((TapDateTimeValue) result).getValue().toEpochMilli());
    }
}
