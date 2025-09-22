package com.tapdata.tm.alarm.service.impl;

import com.tapdata.tm.alarm.service.ApiServerAlarmConfig;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import org.springframework.stereotype.Service;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/12 14:36 Create
 * @description
 */
@Service
public class ApiServerAlarmConfigImpl implements ApiServerAlarmConfig {

    @Override
    public AlarmRuleDto config(String key, AlarmKeyEnum alarmKeyEnum) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
