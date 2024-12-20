package io.tapdata.supervisor;

import cn.hutool.core.collection.ConcurrentHashSet;
import io.tapdata.aspect.supervisor.entity.DisposableThreadGroupBase;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.MainMethod;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.supervisor.entity.MemoryLevel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Bean
@MainMethod("start")
public class TaskResourceSupervisorManager implements MemoryFetcher {
    private static final String TAG = TaskResourceSupervisorManager.class.getSimpleName();

    private final ConcurrentHashSet<TaskNodeInfo> taskNodeInfos = new ConcurrentHashSet<>();
    private final Map<ThreadGroup, DisposableNodeInfo> disposableThreadGroupMap = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService cleanTaskNodeThreadGroupPool = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "clean-taskNode-threadGroup-thread"));
    private String userId;
    private String processId;
    private Long lastCleanThreadGroupTime = null;
    public static final Long CHECK_THREAD_GROUP_POOL_PERIOD = 10 * 60L;

    public static final Long CHECK_THREAD_GROUP_POOL_INITIAL_DELAY = 30L;


    public static final String CLEAN_LEAKED_THREAD_GROUP_MINUTES = System.getenv().getOrDefault("CLEAN_LEAKED_THREAD_GROUP_MINUTES", "30");

    public static final String CLEAN_LEAKED_THREAD_GROUP_THRESHOLD = System.getenv().getOrDefault("CLEAN_LEAKED_THREAD_GROUP_THRESHOLD", "1000");


    public void cleanThreadGroup() {
        if (Objects.isNull(lastCleanThreadGroupTime)) {
            lastCleanThreadGroupTime = System.currentTimeMillis();
        }
        List<TaskNodeInfo> leakedList = taskNodeInfos.stream().filter(TaskNodeInfo::hasLaked).collect(Collectors.toList());
        int leakedSize = leakedList.size();
        if (System.currentTimeMillis() - lastCleanThreadGroupTime > TimeUnit.MINUTES.toMillis(Long.parseLong(CLEAN_LEAKED_THREAD_GROUP_MINUTES))
                || leakedSize >= Integer.parseInt(CLEAN_LEAKED_THREAD_GROUP_THRESHOLD)) {
            for (TaskNodeInfo taskNodeInfo : leakedList) {
                destroyTaskNodeInfo(taskNodeInfo);
            }
            lastCleanThreadGroupTime = System.currentTimeMillis();
        }
    }


    public TaskResourceSupervisorManager() {
        cleanTaskNodeThreadGroupPool.scheduleAtFixedRate(this::cleanThreadGroup, CHECK_THREAD_GROUP_POOL_INITIAL_DELAY, CHECK_THREAD_GROUP_POOL_PERIOD, TimeUnit.SECONDS);
    }

    private void start() {
        PDKIntegration.registerMemoryFetcher(TaskResourceSupervisorManager.class.getSimpleName(), this);
    }

    public void addTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        if (Objects.nonNull(taskNodeInfo)){
            taskNodeInfos.add(taskNodeInfo);
        }
    }

    public ConcurrentHashSet<TaskNodeInfo> getTaskNodeInfos() {
        return taskNodeInfos;
    }

    public void removeTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        if (Objects.nonNull(taskNodeInfo)){
            taskNodeInfo.setNodeThreadGroup(null);
            taskNodeInfo.setSupervisorAspectTask(null);
            taskNodeInfo.setNode(null);
            taskNodeInfos.remove(taskNodeInfo);
        }
    }

    public void addDisposableSubscribeInfo(ThreadGroup threadGroup, DisposableNodeInfo info) {
        disposableThreadGroupMap.put(threadGroup, info);
    }

    public void removeDisposableSubscribeInfo(ThreadGroup threadGroup) {
        DisposableNodeInfo nodeInfo = disposableThreadGroupMap.get(threadGroup);
        if (Objects.nonNull(nodeInfo)) {
            nodeInfo.setNodeThreadGroup(null);
            nodeInfo.setAspectConnector(null);
            nodeInfo.setNodeMap(null);
        }
        disposableThreadGroupMap.remove(threadGroup);
    }

    public DisposableNodeInfo getDisposableSubscribeInfo(ThreadGroup threadGroup) {
        return disposableThreadGroupMap.get(threadGroup);
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        if (!MemoryLevel.needMemory(memoryLevel)){
            return null;
        }
        Set<SupervisorAspectTask> aliveTaskSet = new HashSet<>();
        Set<SupervisorAspectTask> leakedTaskSet = new HashSet<>();
        Set<DisposableNodeInfo> aliveConnectorSet = new HashSet<>();
        Set<DisposableNodeInfo> leakedConnectorSet = new HashSet<>();
        this.summary(taskNodeInfos, aliveTaskSet, leakedTaskSet);
        this.summaryDisposable(disposableThreadGroupMap, aliveConnectorSet, leakedConnectorSet);

        AtomicInteger leakedTaskCount = new AtomicInteger();
        List<DataMap> leakedTaskDataMap = leakedTaskSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).filter(map -> {
            if (Objects.nonNull(map) && !map.isEmpty()){
                leakedTaskCount.getAndIncrement();
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }).collect(Collectors.toList());

        AtomicInteger aliveTaskCount = new AtomicInteger();
        List<DataMap> aliveTaskDataMap = aliveTaskSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).filter(map ->{
            if (Objects.nonNull(map) && !map.isEmpty()){
                aliveTaskCount.getAndIncrement();
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }).collect(Collectors.toList());

        AtomicInteger aliveConnectorCount = new AtomicInteger();
        Map<Object, List<DataMap>> aliveConnectorDataMap = aliveConnectorSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).filter(map -> {
            if (Objects.nonNull(map) && !map.isEmpty()){
                aliveConnectorCount.getAndIncrement();
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }).collect(Collectors.groupingBy(map -> map.get(DisposableThreadGroupBase.MODE_KEY)));

        AtomicInteger lakedConnectorCount = new AtomicInteger();
        Map<Object, List<DataMap>> leakedConnectorDataMap = leakedConnectorSet.stream().filter(Objects::nonNull).map(aspect -> aspect.memory(keyRegex, memoryLevel)).filter(map -> {
            if (Objects.nonNull(map) && !map.isEmpty()){
                lakedConnectorCount.getAndIncrement();
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }).collect(Collectors.groupingBy(map -> map.get(DisposableThreadGroupBase.MODE_KEY)));
        int total = leakedTaskCount.get() + aliveTaskCount.get() + lakedConnectorCount.get() + aliveConnectorCount.get();
        return DataMap.create().keyRegex(keyRegex)
                .kv("mode", MemoryLevel.description(memoryLevel))
                .kv("aliveTaskCount", aliveTaskCount.get())
                .kv("aliveTasks", aliveTaskDataMap)
                .kv("leakedTaskCount", leakedTaskCount.get())
                .kv("leakedTasks", leakedTaskDataMap)
                .kv("aliveConnectorCount", aliveConnectorCount.get())
                .kv("aliveConnectors", aliveConnectorDataMap)
                .kv("leakedConnectorCount", lakedConnectorCount.get())
                .kv("leakedConnectors", leakedConnectorDataMap)
                .kv("msg", total > 0 ? "Succeed" :" The thread occupancy of the corresponding resource has been released normally, and the corresponding information cannot be provided temporarily. Retrieve the summary data. ");
    }

    protected void summary(Set<TaskNodeInfo> summarySet, Set<SupervisorAspectTask> aliveSet, Set<SupervisorAspectTask> leakedSet) {
        for (TaskNodeInfo taskNodeInfo : summarySet) {
            if (destroyTaskNodeInfo(taskNodeInfo)) continue;
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

    public boolean destroyTaskNodeInfo(TaskNodeInfo taskNodeInfo) {
        synchronized (taskNodeInfo.getTaskNodeInfoLock()){
            if (taskNodeInfo.isHasLaked()) {
                try {
                    taskNodeInfo.getNodeThreadGroup().destroy();
                    taskNodeInfo.setHasLeaked(Boolean.FALSE);
                    taskNodeInfo.getSupervisorAspectTask().getThreadGroupMap().remove(taskNodeInfo.getNodeThreadGroup());
                    this.removeTaskSubscribeInfo(taskNodeInfo);
                    return true;
                } catch (Exception e1) {
                    taskNodeInfo.setHasLeaked(Boolean.TRUE);
                }
            }
        }
        return false;
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
