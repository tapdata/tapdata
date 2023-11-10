package io.tapdata.threadgroup.utils;

import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.ProcessorOnTaskThreadGroup;
import io.tapdata.threadgroup.TaskThreadGroup;

import java.lang.reflect.Field;
import java.util.*;

public class ThreadGroupUtil {
    private static final Class<? extends ThreadGroup>[] DEFAULT_TASK_THREAD = new Class[]{TaskThreadGroup.class};
    private static final Class<? extends ThreadGroup>[] DEFAULT_NODE_THREAD = new Class[]{ProcessorOnTaskThreadGroup.class, ConnectorOnTaskThreadGroup.class, DisposableThreadGroup.class};
    private final Class<? extends ThreadGroup> fatherClass;
    private final Class<? extends ThreadGroup>[] subClass;

    public static final ThreadGroupUtil THREAD_GROUP_TASK = ThreadGroupUtil.create(DEFAULT_TASK_THREAD[0],DEFAULT_NODE_THREAD);

    public static ThreadGroupUtil create(Class<? extends ThreadGroup> fatherClass, Class<? extends ThreadGroup>[] subClass) {
        return new ThreadGroupUtil(fatherClass, subClass);
    }

    private ThreadGroupUtil(Class<? extends ThreadGroup> fatherClass, Class<? extends ThreadGroup>[] subClass) {
        this.fatherClass = fatherClass;
        this.subClass = subClass;
    }

    private boolean equalsWithFatherClass(ThreadGroup target) {
        return null != target && target.getClass().equals(fatherClass);
    }

    private boolean containsInSubClass(ThreadGroup target) {
        return ThreadGroupUtil.containsInThreadGroups(target, subClass);
    }

    public Map<ThreadGroup, Set<ThreadGroup>> group(Thread current) {
        if (null == current) return new HashMap<>();
        return group(current.getThreadGroup());
    }

    public Map<ThreadGroup, Set<ThreadGroup>> group(ThreadGroup target) {
        Map<ThreadGroup, Set<ThreadGroup>> groupMap = new HashMap<>();
        return groupThreadGroup(groupMap, target);
    }

    public Map<ThreadGroup, Set<ThreadGroup>> groupCurrent() {
        return group(Thread.currentThread());
    }

    public Map<ThreadGroup, Set<ThreadGroup>> groupAll() {
        ThreadGroup group = systemThreadGroup();
        ThreadGroup[] threadGroups = threadGroups(group);
        Map<ThreadGroup, Set<ThreadGroup>> groupMap = new HashMap<>();
        for (ThreadGroup threadGroup : threadGroups) {
            group(groupMap, threadGroup);
        }
        return groupMap;
    }

    private void group(Map<ThreadGroup, Set<ThreadGroup>> groupMap, ThreadGroup threadGroup) {
        groupThreadGroup(groupMap, threadGroup);
        ThreadGroup[] threadGroups = threadGroups(threadGroup);
        for (ThreadGroup group : threadGroups) {
            group(groupMap, group);
        }
    }

    private Map<ThreadGroup, Set<ThreadGroup>> groupThreadGroup(Map<ThreadGroup, Set<ThreadGroup>> groupMap, ThreadGroup target) {
        while (null != target) {
            parentGroups(groupMap, target);
            target = target.getParent();
        }
        return groupMap;
    }

    private void parentGroups(Map<ThreadGroup, Set<ThreadGroup>> groupMap, ThreadGroup target) {
        if (null == target) return;
        if (this.equalsWithFatherClass(target)) {
            groupMap.computeIfAbsent(target, g -> new HashSet<>());
        } else if (this.containsInSubClass(target)) {
            Optional.ofNullable(getFatherThreadGroup(target)).ifPresent(g -> {
                        Set<ThreadGroup> groups = groupMap.computeIfAbsent(g, g1 -> new HashSet<>());
                        groups.add(target);
                    }
            );
        }
    }

    private ThreadGroup getFatherThreadGroup(ThreadGroup threadGroup) {
        if (null == threadGroup) return null;
        if (this.equalsWithFatherClass(threadGroup)) {
            return fatherClass.cast(threadGroup);
        } else {
            return getFatherThreadGroup(threadGroup.getParent());
        }
    }

    public ThreadGroup systemThreadGroup() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        while (null != group && !"main".equals(group.getName())) {
            if (null != group.getParent()) {
                group = group.getParent();
            } else {
                break;
            }
        }
        return group;
    }

    public static Thread[] groupThreads(ThreadGroup threadGroup) {
        try {
            Class<ThreadGroup> groupClass = ThreadGroup.class;
            Field threads = groupClass.getDeclaredField("threads");
            return (Thread[]) Optional.ofNullable(threads.get(threadGroup)).orElse(new Thread[0]);
        } catch (Exception e) {
            return new Thread[0];
        }
    }

    public static ThreadGroup[] threadGroups(ThreadGroup threadGroup) {
        try {
            Class<ThreadGroup> groupClass = ThreadGroup.class;
            Field threads = groupClass.getDeclaredField("groups");
            return (ThreadGroup[]) Optional.ofNullable(threads.get(threadGroup)).orElse(new ThreadGroup[0]);
        } catch (Exception e) {
            return new ThreadGroup[0];
        }
    }

    public ThreadGroup currentThreadGroup(Thread thread,Class<? extends ThreadGroup>[] groups){
        ThreadGroup threadGroup = thread.getThreadGroup();
        while (!containsInThreadGroups(threadGroup, groups)) {
            if (null == threadGroup.getParent()) break;
            threadGroup = threadGroup.getParent();
        }
        return threadGroup;
    }

    private static boolean containsInThreadGroups(ThreadGroup thread, Class<? extends ThreadGroup>[] groups){
        if (Objects.isNull(thread)) return Boolean.FALSE;
        for (Class<? extends ThreadGroup> aClass : groups) {
            if (thread.getClass().equals(aClass)) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    public static Class<? extends ThreadGroup>[] getDefaultTaskThreadGroupClass() {
        return DEFAULT_TASK_THREAD;
    }

    public static Class<? extends ThreadGroup>[] getDefaultNodeThreadGroupClass() {
        return DEFAULT_NODE_THREAD;
    }
}
