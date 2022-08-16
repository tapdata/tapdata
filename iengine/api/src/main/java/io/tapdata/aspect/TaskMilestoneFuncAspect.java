package io.tapdata.aspect;

import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import org.apache.logging.log4j.Logger;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 16:46 Create
 */
public class TaskMilestoneFuncAspect extends DataNodeAspect<TaskMilestoneFuncAspect> {

    private MilestoneStage stage;
    private MilestoneStatus status;

    public MilestoneStage getStage() {
        return stage;
    }

    public MilestoneStatus getStatus() {
        return status;
    }

    public TaskMilestoneFuncAspect stage(MilestoneStage stage) {
        this.stage = stage;
        return this;
    }

    public TaskMilestoneFuncAspect status(MilestoneStatus status) {
        this.status = status;
        return this;
    }

    public static AspectInterceptResult execute(DataProcessorContext dataProcessorContext, MilestoneStage stage, MilestoneStatus status) {
        return AspectUtils.executeAspect(TaskMilestoneFuncAspect.class, () -> new TaskMilestoneFuncAspect()
                .dataProcessorContext(dataProcessorContext).stage(stage).status(status));
    }

    public static AspectInterceptResult execute(DataProcessorContext dataProcessorContext, MilestoneStage stage, MilestoneStatus status, Logger logger) {
        try {
            return AspectUtils.executeAspect(TaskMilestoneFuncAspect.class, () -> new TaskMilestoneFuncAspect()
                    .dataProcessorContext(dataProcessorContext).stage(stage).status(status));
        } catch (Exception e) {
            logger.warn(e);
            return null;
        }
    }
}
