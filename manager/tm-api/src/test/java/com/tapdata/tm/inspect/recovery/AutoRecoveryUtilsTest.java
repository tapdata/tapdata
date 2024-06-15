package com.tapdata.tm.inspect.recovery;

import com.tapdata.tm.inspect.constant.InspectMethod;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * 自动修复工具类测试
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/14 14:15 Create
 */
class AutoRecoveryUtilsTest {
    @Nested
    class CheckCanRecoveryTest {
        @Test
        void testTrue() {
            InspectDto inspectDto = new InspectDto();
            inspectDto.setInspectMethod(InspectMethod.FIELD.getValue());
            inspectDto.setStatus(InspectStatusEnum.DONE.getValue());
            inspectDto.setResult("failed");
            inspectDto.setFlowId("test-flow-id");

            List<String> errors = AutoRecoveryUtils.checkCanRecovery(inspectDto);
            Assertions.assertTrue(errors.isEmpty());

            inspectDto.setInspectMethod(InspectMethod.JOINTFIELD.getValue());
            errors = AutoRecoveryUtils.checkCanRecovery(inspectDto);
            Assertions.assertTrue(errors.isEmpty());
        }

        @Test
        void testFalse() {
            InspectDto inspectDto = new InspectDto();
            inspectDto.setInspectMethod(InspectMethod.CDC_COUNT.getValue());
            inspectDto.setStatus(InspectStatusEnum.ERROR.getValue());
            inspectDto.setResult("error");

            List<String> errors = AutoRecoveryUtils.checkCanRecovery(inspectDto);
            Assertions.assertFalse(errors.isEmpty());

            inspectDto.setFlowId("");
            errors = AutoRecoveryUtils.checkCanRecovery(inspectDto);
            Assertions.assertFalse(errors.isEmpty());
        }
    }
}
