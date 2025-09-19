package com.tapdata.tm.apiServer.check;

import com.tapdata.tm.apiServer.service.check.CheckCPUMemoryBase;
import com.tapdata.tm.apiServer.service.check.UsageInfo;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.springframework.stereotype.Service;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/16 10:08 Create
 * @description
 */
@Service
public class RealTimeOfApiServerCpuUsageWarn implements CheckCPUMemoryBase {
    @Override
    public void check(UsageInfo usageInfo) {
        //do nothing
    }

    @Override
    public AlarmKeyEnum type() {
        return AlarmKeyEnum.API_SERVER_CPU_USAGE_WARN;
    }
}
