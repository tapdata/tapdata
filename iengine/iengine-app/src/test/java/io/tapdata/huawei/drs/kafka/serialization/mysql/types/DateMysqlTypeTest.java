package io.tapdata.huawei.drs.kafka.serialization.mysql.types;

import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.huawei.drs.kafka.AbsTypeTest;
import org.junit.jupiter.api.Assertions;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/27 15:27 Create
 */
class DateMysqlTypeTest extends AbsTypeTest<DateMysqlType, TapDateValue> {
    static final String TIME_STRING = "2025-02-27";

    public DateMysqlTypeTest() {
        super(TapDateValue.class, new TapStringValue("中文"), TIME_STRING, TIME_STRING.getBytes(), 1);
    }

    @Override
    protected DateMysqlType init() {
        return new DateMysqlType();
    }

    @Override
    protected void assertDecodeBytes(Object result) {
        Assertions.assertEquals(mockBytes, result);
    }

    @Override
    protected void assertDecodeString(Object result) {
        Assertions.assertInstanceOf(TapDateValue.class, result);
        Assertions.assertEquals(TIME_STRING, ((TapDateValue) result).getValue().toFormatString("yyyy-MM-dd"));
    }

    @Override
    protected void assertDecodeInteger(Object result) {
        Assertions.assertEquals(mockInteger, result);
    }
}
