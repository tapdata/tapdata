package io.tapdata.supervisor;

import cn.hutool.core.collection.ConcurrentHashSet;
import io.tapdata.aspect.supervisor.entity.DisposableThreadGroupBase;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Bean
@MainMethod("start")
public class TaskResourceSupervisorManager implements MemoryFetcher {
    private static final String TAG = TaskResourceSupervisorManager.class.getSimpleName();

    private final ConcurrentHashSet<TaskNodeInfo> taskNodeInfos = new ConcurrentHashSet<>();
    private final Map<ThreadGroup, DisposableNodeInfo> disposableThreadGroupMap = new ConcurrentHashMap<>();
    private String userId;
    private String processId;

    public TaskResourceSupervisorManager() {
    }

    private void start() {
        PDKIntegration.registerMemoryFetcher(TaskResourceSupervisorManager.class.getSimpleName(), this);
    }

    public void addTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        taskNodeInfos.add(taskNodeInfo);
    }

    public void removeTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        taskNodeInfos.remove(taskNodeInfo);
    }

    public void addDisposableSubscribeInfo(ThreadGroup threadGroup, DisposableNodeInfo info) {
        disposableThreadGroupMap.put(threadGroup, info);
    }

    public void removeDisposableSubscribeInfo(ThreadGroup threadGroup) {
        disposableThreadGroupMap.remove(threadGroup);
    }

    public DisposableNodeInfo getDisposableSubscribeInfo(ThreadGroup threadGroup) {
        return disposableThreadGroupMap.get(threadGroup);
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        Set<SupervisorAspectTask> aliveTaskSet = new HashSet<>();
        Set<SupervisorAspectTask> leakedTaskSet = new HashSet<>();
        Set<DisposableNodeInfo> aliveConnectorSet = new HashSet<>();
        Set<DisposableNodeInfo> leakedConnectorSet = new HashSet<>();
        this.summary(taskNodeInfos, aliveTaskSet, leakedTaskSet);
        this.summaryDisposable(disposableThreadGroupMap, aliveConnectorSet, leakedConnectorSet);
        return DataMap.create().keyRegex(keyRegex)
                .kv("aliveTaskCount", aliveTaskSet.size())
                .kv("aliveTasks", aliveTaskSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).collect(Collectors.toList()))
                .kv("leakedTaskCount", leakedTaskSet.size())
                .kv("leakedTasks", leakedTaskSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).collect(Collectors.toList()))
                .kv("aliveConnectorCount", aliveConnectorSet.size())
                .kv("aliveConnectors", aliveConnectorSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).collect(Collectors.groupingBy(map -> map.get(DisposableThreadGroupBase.MODE_KEY))))
                .kv("leakedConnectorCount", leakedConnectorSet.size())
                .kv("leakedConnectors", leakedConnectorSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).collect(Collectors.groupingBy(map -> map.get(DisposableThreadGroupBase.MODE_KEY))));
    }

    private void summary(Set<TaskNodeInfo> summarySet, Set<SupervisorAspectTask> aliveSet, Set<SupervisorAspectTask> leakedSet) {
        for (TaskNodeInfo taskNodeInfo : summarySet) {
            if (taskNodeInfo.isHasLaked()) {
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
            if (Objects.isNull(aspectTask)) {
                continue;
            }
            if (!taskNodeInfo.isHasLaked()) {
                aliveSet.add(aspectTask);
            } else {
                leakedSet.add(aspectTask);
            }
        }
    }

    private void summaryDisposable(Map<ThreadGroup, DisposableNodeInfo> disposableMap, Set<DisposableNodeInfo> aliveConnectorSet, Set<DisposableNodeInfo> leakedConnectorSet) {
        Iterator<Map.Entry<ThreadGroup, DisposableNodeInfo>> iterator = disposableMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<ThreadGroup, DisposableNodeInfo> infoEntry = iterator.next();
            DisposableNodeInfo info = infoEntry.getValue();
            if (Objects.isNull(info)) {
                return;
            }
            if (info.isHasLaked()) {
                try {
                    info.getNodeThreadGroup().destroy();
                    info.setHasLaked(Boolean.FALSE);
                    info.setAspectConnector(null);
                    info.setNodeThreadGroup(null);
                    iterator.remove();
                    return;
                } catch (Exception e1) {
                    info.setHasLaked(Boolean.TRUE);
                }
            }
            if (!info.isHasLaked()) {
                aliveConnectorSet.add(info);
            } else {
                leakedConnectorSet.add(info);
            }
        }
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

}
