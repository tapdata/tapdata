package com.tapdata.tm.inspect.recovery;

import com.tapdata.tm.inspect.constant.InspectMethod;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据自动修复工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/12 12:15 Create
 */
public class AutoRecoveryUtils {

    public static List<String> checkCanRecovery(InspectDto inspectDto) {
        List<String> errorCodes = new ArrayList<>();
        if (!(
            InspectMethod.FIELD.getValue().equals(inspectDto.getInspectMethod())
                || InspectMethod.JOINTFIELD.getValue().equals(inspectDto.getInspectMethod())
        )) {
            errorCodes.add("Inspect.Recovery.NotFieldMethod");
        }
        if (!InspectStatusEnum.DONE.getValue().equals(inspectDto.getStatus()) && !InspectStatusEnum.WAITING.getValue().equals(inspectDto.getStatus())) {
            errorCodes.add("Inspect.Recovery.StatusNotDone");
        }
        if (!InspectResultEnum.FAILED.getValue().equals(inspectDto.getResult())) {
            errorCodes.add("Inspect.Recovery.ResultNotFound");
        }
        if (null == inspectDto.getFlowId() || inspectDto.getFlowId().isEmpty()) {
            errorCodes.add("Inspect.Recovery.IsNotWithTask");
        }
        return errorCodes;
    }
}
