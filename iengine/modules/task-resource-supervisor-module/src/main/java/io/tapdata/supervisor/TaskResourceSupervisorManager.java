package io.tapdata.supervisor;

import cn.hutool.core.collection.ConcurrentHashSet;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.proxy.data.NewDataReceived;
import io.tapdata.modules.api.service.SkeletonService;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.supervisor.entity.ClassOnThread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Bean
@MainMethod("start")
public class TaskResourceSupervisorManager implements MemoryFetcher {
    private static final String TAG = TaskResourceSupervisorManager.class.getSimpleName();

    private final ConcurrentHashSet<TaskNodeInfo> taskNodeInfos = new ConcurrentHashSet<>();
    private ConcurrentHashMap<String, List<TaskNodeInfo>> typeConnectionIdSubscribeInfosMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TaskNodeInfo> taskIdTaskSubscribeInfoMap = new ConcurrentHashMap<>();

    private String userId;
    private String processId;
    @Bean
    private ClassLifeCircleMonitor<ClassOnThread> classLifeCircleMonitor;

    @Bean
    private SkeletonService skeletonService;

    public TaskResourceSupervisorManager() {

    }

    private void start() {
        PDKIntegration.registerMemoryFetcher(TaskResourceSupervisorManager.class.getSimpleName(), this);
    }

    private void handleNewDataReceived(String contentType, OutgoingData outgoingData) {
        NewDataReceived newDataReceived = (NewDataReceived) outgoingData.getMessage();
        if (newDataReceived != null && newDataReceived.getSubscribeIds() != null) {
            for (String subscribeId : newDataReceived.getSubscribeIds()) {
                List<TaskNodeInfo> taskNodeInfoList = typeConnectionIdSubscribeInfosMap.get(subscribeId);
                if (taskNodeInfoList != null) {
                    for (TaskNodeInfo taskNodeInfo : taskNodeInfoList) {
                        if (taskNodeInfo.supervisorAspectTask.streamReadConsumer != null) {
                        }
                        // @TODO taskSubscribeInfo.supervisorAspectTask.enableFetchingNewData(subscribeId);
                        else
                            TapLogger.debug(TAG, "streamRead is not started yet, new data request will be ignored for task {}", taskNodeInfo.getSupervisorAspectTask().getTaskId());
                    }
                }
                //TODO
            }
        }
    }

    public void addTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        taskNodeInfos.add(taskNodeInfo);
        if (taskNodeInfo.getSupervisorAspectTask().getTaskId() != null) {
            taskIdTaskSubscribeInfoMap.putIfAbsent(taskNodeInfo.getSupervisorAspectTask().getTaskId(), taskNodeInfo);
        }

    }

    public void removeTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        taskNodeInfos.remove(taskNodeInfo);
        taskIdTaskSubscribeInfoMap.remove(taskNodeInfo.getSupervisorAspectTask().getTaskId());

    }

    public ConcurrentHashMap<String, List<TaskNodeInfo>> getTypeConnectionIdSubscribeInfosMap() {
        return typeConnectionIdSubscribeInfosMap;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        Set<SupervisorAspectTask> aliveTaskSet = new HashSet<>();
        Set<SupervisorAspectTask> leakedTaskSet = new HashSet<>();
        for (TaskNodeInfo taskNodeInfo : taskNodeInfos) {
            if (taskNodeInfo.isHasLaked()){
                try {
                    taskNodeInfo.getNodeThreadGroup().destroy();
                    taskNodeInfo.setHasLaked(Boolean.FALSE);
                    taskNodeInfo.getSupervisorAspectTask().getThreadGroupMap().remove(taskNodeInfo.getNodeThreadGroup());
                    taskNodeInfo.setSupervisorAspectTask(null);
                    taskNodeInfo.setNodeThreadGroup(null);
                    continue;
                } catch (Exception e1) {
                    taskNodeInfo.setHasLaked(Boolean.TRUE);
                }
            }
            SupervisorAspectTask aspectTask = taskNodeInfo.getSupervisorAspectTask();
            if (Objects.isNull(aspectTask)){
                continue;
            }
            if (!taskNodeInfo.isHasLaked()) {
                aliveTaskSet.add(aspectTask);
            } else {
                leakedTaskSet.add(aspectTask);
            }
        }
        return DataMap.create().keyRegex(keyRegex)
                .kv("aliveTaskCount", aliveTaskSet.size())
                .kv("aliveTasks", aliveTaskSet.stream().filter(Objects::nonNull).map(spect -> spect.memory(keyRegex, memoryLevel)).collect(Collectors.toList()))
                .kv("leakedTaskCount", leakedTaskSet.size())
                .kv("leakedTasks", leakedTaskSet.stream().filter(Objects::nonNull).map(spect -> spect.memory(keyRegex, memoryLevel)).collect(Collectors.toList()))
                .kv("aliveConnectorCount", 0)
                .kv("aliveConnectors", new ArrayList<>())
                .kv("leakedConnectorCount", 0)
                .kv("leakedConnectors", new ArrayList<>());
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }
}
