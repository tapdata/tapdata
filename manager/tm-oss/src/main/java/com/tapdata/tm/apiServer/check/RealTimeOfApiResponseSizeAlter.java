package com.tapdata.tm.apiServer.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/15 18:15 Create
 * @description
 */
@Service
public class RealTimeOfApiResponseSizeAlter implements com.tapdata.tm.apiServer.service.check.RealTimeOfApiResponseSizeAlter {
    @Override
    public void check(List<Map<String, Long>> apiReqBytesList) {

    }

    @Override
    public AlarmKeyEnum type() {
        return AlarmKeyEnum.API_SERVER_API_RESPONSE_SIZE_ALTER;
    }
}
