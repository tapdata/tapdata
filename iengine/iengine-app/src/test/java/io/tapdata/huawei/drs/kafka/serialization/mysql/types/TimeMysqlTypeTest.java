package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 15:30 Create
 */
class TimeMysqlTypeTest extends AbsTypeTest<TimeMysqlType, TapTimeValue> {
    static final String TIME_STRING = "00:00:01";

    public TimeMysqlTypeTest() {
        super(TapTimeValue.class, new TapStringValue("中文"), TIME_STRING, TIME_STRING.getBytes(), 1);
    }

    @Override
    protected TimeMysqlType init() {
        return new TimeMysqlType();
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertEquals(mockBytes, result);
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapTimeValue.class, result);
        Assertions.assertEquals(TIME_STRING, ((TapTimeValue) result).getValue().toTimeStr());
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertEquals(mockInteger, result);
    }
}
