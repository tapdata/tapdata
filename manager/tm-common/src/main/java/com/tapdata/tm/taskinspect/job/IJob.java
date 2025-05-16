package com.tapdata.tm.taskinspect.job;

/**
 * 任务接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/19 15:53 Create
 */
public interface IJob extends Runnable {

    /**
     * 停止工作线程，接口应当立即返回结果
     *
     * @return 是否完全退出
     */
    boolean stop();
}
