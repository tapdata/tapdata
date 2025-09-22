package com.tapdata.tm.apiServer.service.check;

import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;

public interface ApiServerCheckBase {

    AlarmKeyEnum type();

    default int sort() {
        return 0;
    }
}
