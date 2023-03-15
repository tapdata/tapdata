package io.tapdata.supervisor;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.PDKNodeInitAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.supervisor.entity.ClassOnThread;

import java.util.Date;
import java.util.List;
import java.util.Map;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC}, ignoreErrors = false, order = 1)
public class SupervisorAspectTask extends AbstractAspectTask {
    private static final String TAG = SupervisorAspectTask.class.getSimpleName();
    @Bean
    TapConnectorSupervisorManager tapConnectorSupervisorManager;
    StreamReadConsumer streamReadConsumer;
    private boolean isStarted;
    Long taskStartTime;
    String taskName;
    private StreamReadFuncAspect streamReadFuncAspect;
    private final TaskNodeInfo taskNodeInfo = new TaskNodeInfo();

    public SupervisorAspectTask() {
        observerHandlers.register(PDKNodeInitAspect.class, this::handlePDKNodeInit);
        observerHandlers.register(StreamReadFuncAspect.class, this::handleStreamRead);
    }

    private Void handleStreamRead(StreamReadFuncAspect aspect) {
        return null;
    }

    private Void handlePDKNodeInit(PDKNodeInitAspect aspect) {
        return null;
    }

    @Override
    public void onStart(TaskStartAspect startAspect) {
        super.onStart(startAspect);
        isStarted = true;
        TaskDto taskDto = startAspect.getTask();
        taskStartTime = System.nanoTime();
        if(taskDto != null && taskDto.getId() != null && taskDto.getDag() != null) {
            taskNodeInfo.taskId = taskDto.getId().toString();
            taskNodeInfo.supervisorAspectTask = this;
            tapConnectorSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
        }
    }

    @Override
    public void onStop(TaskStopAspect stopAspect) {
        super.onStop(stopAspect);
        ClassLifeCircleMonitor circleMonitor = InstanceFactory.instance(ClassLifeCircleMonitor.class);
        Map<Object, ClassOnThread> summary = circleMonitor.summary();
        isStarted = false;
        tapConnectorSupervisorManager.removeTaskSubscribeInfo(taskNodeInfo);
        if(streamReadFuncAspect != null)
            streamReadFuncAspect.noMoreWaitRawData();
    }

    @Override
    public List<Class<? extends Aspect>> observeAspects() {
        return super.observeAspects();
    }

    @Override
    public List<Class<? extends Aspect>> interceptAspects() {
        return super.interceptAspects();
    }

    @Override
    public void onObserveAspect(Aspect aspect) {
        super.onObserveAspect(aspect);
    }

    @Override
    public AspectInterceptResult onInterceptAspect(Aspect aspect) {
        return super.onInterceptAspect(aspect);
    }

    @Override
    public TaskDto getTask() {
        return super.getTask();
    }

    @Override
    public void setTask(TaskDto task) {
        super.setTask(task);
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return super.memory(keyRegex, memoryLevel)
                .kv("name","")
                //.kv("associateId","")
                //.kv("id","")
                //.kv("threadCount",0)
                //.kv("threads",null)
                //.kv("resources",null)
                .kv("taskStartTime", taskStartTime != null ? new Date(taskStartTime) : null)
                .kv("connectors", taskNodeInfo.memory(keyRegex, memoryLevel));
    }
}
