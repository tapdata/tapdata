package io.tapdata.supervisor;

import java.util.Map;

public interface ClassLifeCircleMonitor<T> {

    /**
     * 关联this对象以及当前线程, 创建时间
     *
     * @param thisObj 关联this对象
     */
    void instanceStarted(Object thisObj);

    /**
     * 释放this对象的关联
     *
     * @param thisObj 待释放this对象
     */
    void instanceEnded(Object thisObj);

    Map<Object, T> summary();

}