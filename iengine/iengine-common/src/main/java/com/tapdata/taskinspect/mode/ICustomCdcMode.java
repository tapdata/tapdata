package com.tapdata.taskinspect.mode;

import com.tapdata.tm.taskinspect.config.CustomCdc;

import java.util.LinkedHashMap;

/**
 * 增量校验交互接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/18 12:16 Create
 */
public interface ICustomCdcMode {

    default void refresh(CustomCdc config) throws InterruptedException {
    }

    /**
     * @param cdcReadTs 增量读取时间
     * @param cdcOpTs   增量变更时间
     * @param tableName 表名
     * @param keys      行主键
     */
    default void acceptCdcEvent(long cdcReadTs, long cdcOpTs, String tableName, LinkedHashMap<String, Object> keys) {

    }

    default boolean stop() {
        return true;
    }
}
