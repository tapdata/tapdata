package com.tapdata.taskinspect;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.vo.ManualOperationVo;

import java.util.List;

/**
 * 任务内校验-任务交互接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/5/8 11:18 Create
 */
public interface ITaskInspect extends AutoCloseable {

    void setSyncDelay(long syncDelay);

    void acceptCdcEvent(DataProcessorContext dataProcessorContext, TapdataEvent event);

    void refresh(TaskInspectConfig config) throws InterruptedException;

    void manualCheck(ManualOperationVo param);

    void manualRecover(ManualOperationVo param);

    void exportRecoverSql(ManualOperationVo param);

    boolean stop(boolean force);

    void stop(long timeout) throws InterruptedException;
}
