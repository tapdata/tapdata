package io.tapdata.huawei.drs.kafka;

import com.tapdata.huawei.drs.kafka.FromDBType;
import com.tapdata.huawei.drs.kafka.StoreType;
import io.tapdata.huawei.drs.kafka.serialization.JsonSerialization;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/25 18:57 Create
 */
class ISerializationTest {

    @Test
    void testCreateJson() {
        ISerialization serialization = ISerialization.create(StoreType.JSON.name(), FromDBType.MYSQL.name());
        Assertions.assertTrue(serialization instanceof JsonSerialization);
    }

    @Test
    void testUnSupport() {
        Assertions.assertThrows(RuntimeException.class, () -> ISerialization.create(StoreType.JSON_C.name(), FromDBType.MYSQL.name()));
    }
}
