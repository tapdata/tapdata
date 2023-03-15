package io.tapdata.supervisor;

import cn.hutool.core.collection.ConcurrentHashSet;
import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.modules.api.net.data.OutgoingData;
import io.tapdata.modules.api.proxy.data.NewDataReceived;
import io.tapdata.modules.api.service.SkeletonService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Bean
public class TapConnectorSupervisorManager implements MemoryFetcher {
    private static final String TAG = TapConnectorSupervisorManager.class.getSimpleName();

    private final ConcurrentHashSet<TaskNodeInfo> taskNodeInfos = new ConcurrentHashSet<>();
    private ConcurrentHashMap<String, List<TaskNodeInfo>> typeConnectionIdSubscribeInfosMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TaskNodeInfo> taskIdTaskSubscribeInfoMap = new ConcurrentHashMap<>();

    private String userId;
    private String processId;

    @Bean
    private SkeletonService skeletonService;

    public TapConnectorSupervisorManager() {

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
                            TapLogger.debug(TAG, "streamRead is not started yet, new data request will be ignored for task {}", taskNodeInfo.taskId);
                    }
                }
                //TODO
            }
        }
    }

    public void addTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        taskNodeInfos.add(taskNodeInfo);
        if (taskNodeInfo.taskId != null) {
            taskIdTaskSubscribeInfoMap.putIfAbsent(taskNodeInfo.taskId, taskNodeInfo);
        }

    }

    public void removeTaskSubscribeInfo(TaskNodeInfo taskNodeInfo) {
        taskNodeInfos.remove(taskNodeInfo);
        taskIdTaskSubscribeInfoMap.remove(taskNodeInfo.taskId);

    }

    public void taskSubscribeInfoChanged(TaskNodeInfo taskNodeInfo) {

    }

    private void handleTaskSubscribeInfoAfterComplete() {
//		maxFrequencyLimiter.touch();
//		workingFuture = null;
//		if(needSync.get()) {
//			synchronized (this) {
//				if(workingFuture == null) {
//					workingFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::syncSubscribeIds, 500, TimeUnit.MILLISECONDS);
//				}
//			}
//		}
    }


    public ConcurrentHashMap<String, List<TaskNodeInfo>> getTypeConnectionIdSubscribeInfosMap() {
        return typeConnectionIdSubscribeInfosMap;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        DataMap dataMap = DataMap.create().keyRegex(keyRegex)
                .kv("aliveTaskCount", 0)
                .kv("leakedTaskCount", 0)
                .kv("aliveConnectorCount", 0)
                .kv("leakedConnectorCount", 0)
                .kv("aliveTasks", new ArrayList<>())
                .kv("leakedTasks", new ArrayList<>())
                .kv("aliveConnectors", new ArrayList<>())
                .kv("leakedConnectors", new ArrayList<>());
        return dataMap;
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
