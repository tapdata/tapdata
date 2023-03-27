package io.tapdata.supervisor;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.supervisor.entity.ClassOnThread;
import io.tapdata.supervisor.entity.MemoryLevel;
import io.tapdata.threadgroup.utils.ThreadGroupUtil;

import java.util.*;
import java.util.stream.Collectors;

public class DisposableNodeInfo implements MemoryFetcher {
    public static final int STACK_LENGTH = 10;
    boolean hasLaked;
    private ThreadGroup nodeThreadGroup;
    private DataMap nodeMap;
    private SupervisorAspectDisposable aspectConnector;

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        List<DataMap> threads = new ArrayList<>();
        List<DataMap> resources = new ArrayList<>();
        int threadCount = this.threads(threads, keyRegex, memoryLevel);
        boolean needResource = Objects.isNull(memoryLevel) || !memoryLevel.matches(MemoryLevel.SUMMARY.level());
        int resourcesCount = this.resources(resources, keyRegex, memoryLevel, needResource);
        DataMap dataMap = DataMap.create().keyRegex(keyRegex)/*.prefix(TaskSubscribeInfo.class.getSimpleName())*/
                .kv("threadCount", threadCount)
                .kv("resourcesCount", resourcesCount);
        if (Objects.nonNull(nodeMap) && !nodeMap.isEmpty()) {
            dataMap.putAll(nodeMap);
        }
        String associateId = String.valueOf(dataMap.get("associateId"));
        if (Objects.isNull(memoryLevel) || !memoryLevel.matches(MemoryLevel.SUMMARY.level())) {
            dataMap.kv("resources", resources)
                    .kv("threads", threads);
        }else {
            String doMain = MemoryLevel.SUMMARY.level(memoryLevel);
            String url = String.format("%s%s", doMain, associateId);
            dataMap.kv("accessUri", url);
            //dataMap.kv("tip",String.format("You can access the link %s to obtain the current resource usage (Note: Information may not be available after the resource is released normally).", url));
        }
        return Optional.ofNullable(MemoryLevel.filter(memoryLevel,associateId)).orElse(Boolean.FALSE) ? dataMap : null;
    }

    public boolean isHasLaked() {
        return hasLaked;
    }

    public ThreadGroup getNodeThreadGroup() {
        return nodeThreadGroup;
    }

    public void setNodeThreadGroup(ThreadGroup nodeThreadGroup) {
        this.nodeThreadGroup = nodeThreadGroup;
    }

    public DataMap getNodeMap() {
        return nodeMap;
    }

    public void setNodeMap(DataMap nodeMap) {
        this.nodeMap = nodeMap;
    }

    public void setHasLaked(boolean hasLaked) {
        this.hasLaked = hasLaked;
    }


    private int threads(List<DataMap> threads, String keyRegex, String memoryLevel) {
        Thread[] leakedOrAliveThreads = ThreadGroupUtil.groupThreads(this.nodeThreadGroup);
        int threadCount = 0;
        for (Thread thread : leakedOrAliveThreads) {
            if (null == thread || !thread.isAlive()) continue;
            threadCount++;
            if (!MemoryLevel.SUMMARY.level().equals(memoryLevel)) {
                List<String> stack = new ArrayList<>();
                StackTraceElement[] stackTrace = thread.getStackTrace();
                for (int index = 0; index < Math.min(STACK_LENGTH, stackTrace.length); index++) {
                    StackTraceElement element = stackTrace[index];
                    stack.add(element.toString());
                }
                threads.add(DataMap.create()
                        .keyRegex(keyRegex)
                        .kv("name", thread.getName())
                        .kv("id", thread.getId())
                        .kv("methodStacks", stack)
                );
            }
        }
        return threadCount;
    }

    private int resources(List<DataMap> resources, String keyRegex, String memoryLevel, boolean needResources) {
        ClassLifeCircleMonitor<ClassOnThread> classLifeCircleMonitor = InstanceFactory.instance(ClassLifeCircleMonitor.class);
        Map<Object, ClassOnThread> summary = classLifeCircleMonitor.summary();
        Collection<ClassOnThread> onThreads = summary.values();
        Map<ThreadGroup, List<ClassOnThread>> groupListMap = onThreads.stream().filter(obj -> Objects.nonNull(obj) && Objects.nonNull(obj.getThreadGroup()) && obj.getThreadGroup().equals(nodeThreadGroup)).collect(Collectors.groupingBy(ClassOnThread::getThreadGroup));

        List<DataMap> infoArray = new ArrayList<>();
        int resourcesCount = 0;
        for (Map.Entry<ThreadGroup, List<ClassOnThread>> groupEntry : groupListMap.entrySet()) {
            List<ClassOnThread> thInfos = groupEntry.getValue();
            if (needResources) {
                Map<? extends Class<?>, List<ClassOnThread>> listMap = thInfos.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(info -> info.getThisObj().getClass()));
                for (Map.Entry<? extends Class<?>, List<ClassOnThread>> entry : listMap.entrySet()) {
                    Class<?> clz = entry.getKey();
                    List<ClassOnThread> infos = entry.getValue();
                    DataMap map = DataMap.create().keyRegex(keyRegex);
                    map.kv(clz.getName(), infos.size());
                    List<List<String>> stacks = new ArrayList<>();
                    for (ClassOnThread info : infos) {
                        List<String> stackTrace = info.getStackTrace();
                        stacks.add(stackTrace.subList(0, Math.min(STACK_LENGTH, stackTrace.size()) - 1));
                    }
                    map.kv("stack", stacks);
                    infoArray.add(map);
                }
            }
            resourcesCount += thInfos.size();
        }
        //排序
        if (needResources) {
            infoArray.sort((o1, o2) -> {
                DataMap p1 = (DataMap) o1;
                DataMap p2 = (DataMap) o2;
                int len1 = ((List<Object>) Optional.ofNullable(p1.get("stack")).orElse(new ArrayList<>())).size();
                int len2 = ((List<Object>) Optional.ofNullable(p2.get("stack")).orElse(new ArrayList<>())).size();
                return len2 - len1;
            });
            resources.addAll(infoArray);
        }
        resources.addAll(infoArray);
        return resourcesCount;
    }

    public SupervisorAspectDisposable getAspectConnector() {
        return aspectConnector;
    }

    public void setAspectConnector(SupervisorAspectDisposable aspectConnector) {
        this.aspectConnector = aspectConnector;
    }
}
