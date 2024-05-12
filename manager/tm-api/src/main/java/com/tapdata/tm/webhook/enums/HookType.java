package com.tapdata.tm.webhook.enums;

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

}
