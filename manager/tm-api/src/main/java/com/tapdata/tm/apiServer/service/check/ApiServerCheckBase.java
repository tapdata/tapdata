package com.tapdata.tm.apiServer.service.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;

public interface ApiServerCheckBase {
    default boolean enable() {
        return true;
    }

    AlarmKeyEnum type();

    default int sort() {
        return 0;
    }
}
