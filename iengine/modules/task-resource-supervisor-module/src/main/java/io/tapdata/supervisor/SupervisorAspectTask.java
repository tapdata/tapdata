package io.tapdata.supervisor;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.*;
import io.tapdata.aspect.supervisor.DataNodeThreadGroupAspect;
import io.tapdata.aspect.supervisor.ProcessNodeThreadGroupAspect;
import io.tapdata.aspect.supervisor.ThreadGroupAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.supervisor.util.TapTaskThreadGroupUtil;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import io.tapdata.threadgroup.utils.ThreadGroupUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC}, ignoreErrors = false, order = 1)
public class SupervisorAspectTask extends AbstractAspectTask {
    private static final String TAG = SupervisorAspectTask.class.getSimpleName();
    @Bean
    TaskResourceSupervisorManager taskResourceSupervisorManager;
    StreamReadConsumer streamReadConsumer;
    private boolean isStarted;
    Long taskStartTime;
    String taskName;
    String taskId;
    String connectorId;

    Map<ThreadGroup, TaskNodeInfo> threadGroupMap = new ConcurrentHashMap<>();


    public SupervisorAspectTask() {
        observerHandlers.register(DataNodeThreadGroupAspect.class, this::handleDataNode);
        observerHandlers.register(ProcessNodeThreadGroupAspect.class, this::handleProcessNode);
    }

    private Void handleDataNode(DataNodeThreadGroupAspect aspect) {
        return this.addAspect(aspect);
    }

    protected Void addAspect(ThreadGroupAspect aspect) {
        connectorId = aspect.getNode().getId();
        Optional.ofNullable(aspect).flatMap(a -> Optional.ofNullable(aspect.getThreadGroup())).ifPresent(t -> {
            TaskNodeInfo taskNodeInfo = taskResourceSupervisorManager.getTaskNodeInfos()
                    .stream().filter(nodeInfo -> nodeInfo.getNodeThreadGroup().equals(aspect.getThreadGroup()))
                    .findAny().orElse(null);
            if (null == taskNodeInfo) {
                taskNodeInfo = new TaskNodeInfo();
                taskNodeInfo.setNodeThreadGroup(t);
                taskNodeInfo.setNode(aspect.getNode());
                taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
            }
            taskNodeInfo.setSupervisorAspectTask(this);
            taskNodeInfo.setAssociateId(aspect.getAssociateId());
            threadGroupMap.put(t, taskNodeInfo);
        });
        return null;
    }

    private Void handleProcessNode(ProcessNodeThreadGroupAspect aspect) {
        return this.addAspect(aspect);
    }

    @Override
    public void onStart(TaskStartAspect startAspect) {
        super.onStart(startAspect);
        isStarted = true;
        TaskDto taskDto = startAspect.getTask();
        taskStartTime = System.nanoTime();
        if (taskDto != null && taskDto.getId() != null && taskDto.getDag() != null) {
            taskId  = taskDto.getId().toString();
            taskName = taskDto.getName();
        }
    }

    @Override
    public void onStop(TaskStopAspect stopAspect) {
        super.onStop(stopAspect);
        isStarted = false;
        if (null != this.threadGroupMap && !this.threadGroupMap.isEmpty()) {
            for (Map.Entry<ThreadGroup, TaskNodeInfo> infoEntry : threadGroupMap.entrySet()) {
                ThreadGroup group = infoEntry.getKey();
                TaskNodeInfo info = infoEntry.getValue();
                synchronized (info){
                    try {
                        group.destroy();
                        info.setHasLeaked(Boolean.FALSE);
                        taskResourceSupervisorManager.removeTaskSubscribeInfo(info);
                        threadGroupMap.remove(group);
                    } catch (Exception e) {
                        info.hasLaked = Boolean.TRUE;
                        //@todo 延时30s再destroy后统计节点上泄露的线程
                    }
                }

            }
        }
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

    public String getTaskId() {
        return taskId;
    }

    public String getConnectorId() {
        return connectorId;
    }

    public Map<ThreadGroup, TaskNodeInfo> getThreadGroupMap() {
        return threadGroupMap;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        List<DataMap> connectors = new ArrayList<>();
        threadGroupMap.forEach(((threadGroup, taskNodeInfo) -> {
            DataMap memory = taskNodeInfo.memory(keyRegex, memoryLevel);
            if (Objects.nonNull(memory)) {
                connectors.add(memory);
            }
        }));
        return connectors.isEmpty() ?
                null :
                super.memory(keyRegex, memoryLevel)
                    .kv("taskName", taskName)
                    .kv("taskId", taskId)
                    .kv("taskStartTime", taskStartTime != null ? new Date(taskStartTime) : null)
                    .kv("connectors", connectors);
    }

    private void group(){
        TapTaskThreadGroupUtil.getDefaultThreadUtil().groupAll();
    }

}
