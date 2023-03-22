package io.tapdata.threadgroup.utils;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.pdk.core.async.AsyncUtils;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.ProcessorOnTaskThreadGroup;
import io.tapdata.threadgroup.TaskThreadGroup;

import java.lang.reflect.Field;
import java.util.*;

public class ThreadGroupUtil {
    public static final Class<? extends ThreadGroup>[] DEFAULT_TASK_THREAD = new Class[]{TaskThreadGroup.class};
    public static final Class<? extends ThreadGroup>[] DEFAULT_NODE_THREAD = new Class[]{ProcessorOnTaskThreadGroup.class, ConnectorOnTaskThreadGroup.class, DisposableThreadGroup.class};
    public static final ThreadGroupUtil THREAD_GROUP_TASK = ThreadGroupUtil.create(DEFAULT_TASK_THREAD[0],DEFAULT_NODE_THREAD);
    private final Class<? extends ThreadGroup> fatherClass;
    private final Class<? extends ThreadGroup>[] subClass;


    private static ThreadGroup getSystemGroup(Thread thread){
        ThreadGroup group = thread.getThreadGroup();
        while (null != group && !"system".equals(group.getName())){
            group = group.getParent();
        }
        return group;
    }

    private boolean isFather(ThreadGroup target) {
        return (Objects.nonNull(target)) && target.getClass().getName().equals(fatherClass.getName());
    }

    private boolean isSub(ThreadGroup target) {
        return this.content(target,subClass);
    }

    public static ThreadGroupUtil create(Class<? extends ThreadGroup> fatherClass, Class<? extends ThreadGroup>[] subClass) {
        return new ThreadGroupUtil(fatherClass, subClass);
    }

    private ThreadGroupUtil(Class<? extends ThreadGroup> fatherClass, Class<? extends ThreadGroup>[] subClass) {
        this.fatherClass = fatherClass;
        this.subClass = subClass;
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
        //Thread[] threads = groupThreads(group);
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
        if (this.isFather(target)) {
            groupMap.computeIfAbsent(target, g -> new HashSet<>());
        } else if (this.isSub(target)) {
            Optional.ofNullable(getFatherThreadGroup(target)).ifPresent(g -> {
                        Set<ThreadGroup> groups = groupMap.computeIfAbsent(g, g1 -> new HashSet<>());
                        groups.add(target);
                    }
            );
        }
    }

    private TaskThreadGroup getFatherThreadGroup(ThreadGroup threadGroup) {
        if (null == threadGroup) return null;
        if (this.isFather(threadGroup)) {
            return (TaskThreadGroup) threadGroup;
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
            threads.setAccessible(Boolean.TRUE);
            return (Thread[]) Optional.ofNullable(threads.get(threadGroup)).orElse(new Thread[0]);
        } catch (Exception e) {
            return new Thread[0];
        }
    }

    public static ThreadGroup[] threadGroups(ThreadGroup threadGroup) {
        try {
            Class<ThreadGroup> groupClass = ThreadGroup.class;
            Field threads = groupClass.getDeclaredField("groups");
            threads.setAccessible(Boolean.TRUE);
            return (ThreadGroup[]) Optional.ofNullable(threads.get(threadGroup)).orElse(new ThreadGroup[0]);
        } catch (Exception e) {
            return new ThreadGroup[0];
        }
    }

    public ThreadGroup currentThreadGroup(Thread thread,Class<? extends ThreadGroup>[] fatherGroups){
        ThreadGroup threadGroup = thread.getThreadGroup();
        while (!content(threadGroup, fatherGroups)) {
            if (null == threadGroup.getParent()) break;
            threadGroup = threadGroup.getParent();
        }
        return threadGroup;
    }

    private static boolean content(ThreadGroup thread,Class<? extends ThreadGroup>[] fatherGroups){
        if (Objects.isNull(thread)) return Boolean.FALSE;
        for (Class<? extends ThreadGroup> aClass : fatherGroups) {
            if (thread.getClass().getName().equals(aClass.getName())) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    //for test thread
    public static void main(String[] args) {
        ThreadGroupUtil util = ThreadGroupUtil.create(TaskThreadGroup.class,new Class[]{ConnectorOnTaskThreadGroup.class, ProcessorOnTaskThreadGroup.class});
        final Object lock = new Object();
        TaskDto taskDto = new TaskDto();
        taskDto.setName("main-sub");
        ThreadGroup group = new TaskThreadGroup(taskDto);
        Thread td = new Thread(() -> {
//            while (true) {
//                synchronized (lock) {
//                    try {
//                        lock.wait(500);
//                    } catch (Exception e) {
//
//                    }
//                }
//            }
        }, "main-thread-1");
        td.start();


        Thread td_sub = new Thread(group, () -> {
//            synchronized (lock) {
//                try {
//                    lock.wait(500);
//                } catch (Exception e) {
//
//                }
//            }
            TaskDto taskDto1 = new TaskDto();
            taskDto1.setName("main-sub-sub");
            DatabaseNode node = new DatabaseNode();
            node.setName("Mysql-target");
            DataProcessorContext context = new DataProcessorContext.DataProcessorContextBuilder().withTaskDto(taskDto1).withNode(node).build();
            ThreadGroup groupSub = new ConnectorOnTaskThreadGroup(context);
            Thread td_sub_sub = new Thread(groupSub,() -> {
//                synchronized (lock) {
//                    try {
                        System.out.println("Node:"+ node.getName());
//                        Map<ThreadGroup, Set<ThreadGroup>> taskThreadGroupSetMap = util.groupAll();
//                        lock.wait(5000);
//                    } catch (Exception e) {
//
//                    }
//                }
            }, "main-sub-sub-thread");
            td_sub_sub.start();
        }, "main-thread-2");
        td_sub.start();

        group.destroy();
        td_sub.isAlive();
//        synchronized (lock) {
//            try {
//                lock.wait(500);
//            } catch (Exception e) {
//
//            }
//        }

    }

    //for test thread pool
    public static void main1(String[] args) {
        ThreadGroupUtil util = ThreadGroupUtil.create(TaskThreadGroup.class,new Class[]{ConnectorOnTaskThreadGroup.class, ProcessorOnTaskThreadGroup.class});
        final Object lock = new Object();
        TaskDto taskDto = new TaskDto();
        taskDto.setName("main-sub");
        ThreadGroup group = new TaskThreadGroup(taskDto);

        ThreadPoolExecutorEx threadPoolExecutor = AsyncUtils.createThreadPoolExecutor("RootTask-" + taskDto.getName(), 1, group, "TAG");
//        threadPoolExecutor.submitSync(() -> {
//            while (true) {
//                synchronized (lock) {
//                    try {
//                        lock.wait(500);
//                    } catch (Exception e) {
//
//                    }
//                }
//            }
//        });


        threadPoolExecutor.submitSync(() -> {
            synchronized (lock) {
                try {
                    lock.wait(500);
                } catch (Exception e) {

                }
            }
            TaskDto taskDto1 = new TaskDto();
            taskDto1.setName("main-sub-sub");
            DataProcessorContext context = new DataProcessorContext.DataProcessorContextBuilder().withTaskDto(taskDto1).build();
            ThreadGroup groupSub = new ConnectorOnTaskThreadGroup(context);
            ThreadPoolExecutorEx tag = AsyncUtils.createThreadPoolExecutor("RootTask-" + taskDto1.getName(), 1, groupSub, "TAG");
            tag.submitSync(() -> {
                synchronized (lock) {
                    try {
                        Map<ThreadGroup, Set<ThreadGroup>> taskThreadGroupSetMap = util.groupAll();
                        lock.wait(500);
                    } catch (Exception e) {

                    }
                }
            });

        });


        synchronized (lock) {
            try {
                lock.wait(500);
            } catch (Exception e) {

            }
        }

    }
}
