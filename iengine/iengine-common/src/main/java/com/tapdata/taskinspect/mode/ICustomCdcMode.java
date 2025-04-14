package com.tapdata.taskinspect.mode;

import com.tapdata.taskinspect.vo.TaskInspectCdcEvent;
import com.tapdata.tm.taskinspect.config.CustomCdc;

/**
 * 增量校验交互接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/18 12:16 Create
 */
public interface ICustomCdcMode {

    default void refresh(CustomCdc config) throws InterruptedException {
    }

    default void acceptCdcEvent(TaskInspectCdcEvent event) {
    }

    default void syncDelay(long delay) {}

    default boolean stop() {
        return true;
    }
}
