package io.tapdata.huawei.drs.kafka;

import io.tapdata.huawei.drs.kafka.types.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/25 18:57 Create
 */
class ITypeTest {

    @Test
    void testDuplicate() {
        Map<String, IType> types = new HashMap<>();
        new StringType("string").append2(types);
        Assertions.assertThrows(RuntimeException.class, () -> new StringType("string").append2(types));
    }

}
