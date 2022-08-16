package io.tapdata.module;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskMilestoneFuncAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.autoinspect.AutoInspectManager;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.milestone.MilestoneStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/22 14:37 Create
 */
@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE}, ignoreErrors = false)
public class AutoInspectAspectTask extends AbstractAspectTask {
    private static final Logger logger = LogManager.getLogger(AutoInspectAspectTask.class);

    private final ClientMongoOperator clientMongoOperator;

    public AutoInspectAspectTask() {
        clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        interceptHandlers.register(TaskMilestoneFuncAspect.class, this::taskMilestoneFuncAspect);
    }


    @Override
    public void onStart(TaskStartAspect startAspect) {
        super.onStart(startAspect);

        TaskDto task = startAspect.getTask();
        AutoInspectManager.start(clientMongoOperator, task);
    }

    @Override
    public void onStop(TaskStopAspect stopAspect) {
        super.onStop(stopAspect);
        AutoInspectManager.call(stopAspect.getTask(), (taskDto, statusCtl) -> {
            if (null != stopAspect.getError()) {
                statusCtl.syncError(stopAspect.getError().getMessage());
            } else {
                statusCtl.syncDone();
            }
            try {
                statusCtl.waitExit(TimeUnit.MINUTES.toMillis(1));
            } catch (InterruptedException | TimeoutException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private AspectInterceptResult taskMilestoneFuncAspect(TaskMilestoneFuncAspect aspect) {
        return AutoInspectManager.call(aspect.getDataProcessorContext().getTaskDto(), (taskDto, statusCtl) -> {
            switch (aspect.getStage()) {
                case READ_SNAPSHOT:
                    if (MilestoneStatus.RUNNING == aspect.getStatus()) {
                        statusCtl.syncInitialing();
                    }
                    break;
                case WRITE_SNAPSHOT:
                    if (MilestoneStatus.FINISH == aspect.getStatus()) {
                        statusCtl.syncInitialized();
                    }
                    break;
                case WRITE_CDC_EVENT:
                    if (MilestoneStatus.RUNNING == aspect.getStatus()) {
                        statusCtl.syncIncremental();
                    }
                    break;
                default:
                    break;
            }
            return null;
        });
    }
}
