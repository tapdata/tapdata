package com.tapdata.taskinspect;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import lombok.Getter;
import org.bson.types.ObjectId;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 任务内校验-上下文
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/13 16:17 Create
 */
public class TaskInspectContext {
    /**
     * 状态上报间隔时间 ms
     */
    @Getter
    private final long reportInterval = 10 * 1000;
    @Getter
    private final String taskId;
    @Getter
    private final TaskDto task;
    @Getter
    private final ClientMongoOperator clientMongoOperator;
    /**
     * 停止状态不可逆，设置为 true 后不可恢复
     */
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    @Getter
    private boolean forceStop = false;

    public TaskInspectContext(TaskDto task, ClientMongoOperator clientMongoOperator) {
        this.task = task;
        this.clientMongoOperator = clientMongoOperator;
        this.taskId = Optional.ofNullable(getTask().getId()).map(ObjectId::toHexString).orElse(null);
    }

    public boolean isStopping() {
        return stopping.get();
    }

    /**
     * 设置停止标志的方法
     *
     * @param force 是否强制停止的标志
     */
    protected void setStop(boolean force) {
        // 将forceStop标志与传入的force参数进行或运算，如果force为true，则forceStop将被设置为true
        this.forceStop = this.forceStop || force;
        // 设置stopping标志为true，表示正在请求停止
        this.stopping.set(true);
    }

}
