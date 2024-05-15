package com.tapdata.tm.webhook.enums;

import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import org.apache.commons.lang3.StringUtils;

public enum HookType {
    ALTER("task.alter", "webHookTaskAlter"),
    PING("webhook.ping", "webKookPingServer"),
    ;
    String hookName;
    String hookBeanName;

    HookType(String hookName, String hookBeanName) {
        this.hookName = hookName;
        this.hookBeanName = hookBeanName;
    }

    public String getHookName() {
        return hookName;
    }

    public String getHookBeanName() {
        return hookBeanName;
    }

    public static String getHookBeanNameByType(String type, String metric) {
        if (StringUtils.isBlank(type)) {
            return ALTER.hookBeanName;
        }
        for (HookType value : values()) {
            if (value.hookName.equals(type)) return value.hookBeanName;
        }
        for (AlarmTypeEnum value : AlarmTypeEnum.values()) {
            if (value.name().equals(type)) return ALTER.hookBeanName;
        }
        for (AlarmKeyEnum value : AlarmKeyEnum.values()) {
            if (value.name().equals(metric)) return ALTER.hookBeanName;
        }
        return ALTER.hookBeanName;
    }

}
