package io.tapdata.threadgroup.utils;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.ProcessorOnTaskThreadGroup;
import io.tapdata.threadgroup.TaskThreadGroup;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadGroupUtilTest {

    /**
     * 测试 ThreadGroupUtil.getFatherThreadGroup 方法
     *  根据子ThreadGroup获取父ThreadGroup
     * */
    @Test
    public void testGetFatherThreadGroup() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = mockTaskThreadGroup();
        new Thread(group, () -> {
            try {
                ThreadGroup groupSub = mockProcessorOnTaskThreadGroup();
                Object fatherThreadGroup = invokerPrivateMethod(
                        util,
                        "getFatherThreadGroup",
                        getArray((Class<?>)ThreadGroup.class),
                        groupSub);
                Assert.assertNotNull(fatherThreadGroup);
                Assert.assertEquals(TaskThreadGroup.class, fatherThreadGroup.getClass());
            } finally {
                count.set(-1);
            }
        }, "main-sub").start();
        while (count.get() >= 0) { }
        count.set(-1);
    }

    /**
     * 测试 ThreadGroupUtil.getFatherThreadGroup 方法
     *  根据子ThreadGroup获取父ThreadGroup, 边界情况：参数为null
     * */
    @Test
    public void testGetFatherThreadGroup0() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = mockTaskThreadGroup();
        new Thread(group, () -> {
            try {
                ThreadGroup groupSub = null;
                Object fatherThreadGroup = invokerPrivateMethod(
                        util,
                        "getFatherThreadGroup",
                        getArray((Class<?>)ThreadGroup.class),
                        groupSub);
                Assert.assertNull(fatherThreadGroup);
            } finally {
                count.set(-1);
            }
        }, "main-sub").start();
        while (count.get() >= 0) { }
        count.set(-1);
    }

    /**
     * 测试 ThreadGroupUtil.getFatherThreadGroup 方法
     *  获取父ThreadGroup, 边界情况：传入 ThreadGroup 的全部父ThreadGroup 均不是 指定父ThreadGroup类型
     * */
    @Test
    public void testGetFatherThreadGroup1() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;//指定了TaskThreadGroup为父类型
        ThreadGroup groupSub = new ThreadGroup("mock-group");
        Object fatherThreadGroup = invokerPrivateMethod(
                util,
                "getFatherThreadGroup",
                getArray((Class<?>)ThreadGroup.class),
                groupSub);
        Assert.assertNull(fatherThreadGroup);
    }

    /**
     * 测试 ThreadGroupUtil.groupThread 方法
     *  执行线程，父线程绑定TaskThreadGroup后启动后再启动ProcessorOnTaskThreadGroup绑定的子线程, 根据子ThreadGroup来找到父子关系, 符合预期情况
     * */
    @Test
    public void testGroupThreadGroup0() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = mockTaskThreadGroup();
        new Thread(group, () -> {
            try {
                ThreadGroup groupSub = mockProcessorOnTaskThreadGroup();
                new Thread(groupSub, () -> {
                    count.incrementAndGet();
                    while (count.get() >= 0) { }
                }, "main-sub-1").start();
                Map<ThreadGroup, Set<ThreadGroup>> groupRes = util.group(groupSub);
                Assert.assertFalse(groupRes.isEmpty());
                Assert.assertTrue(groupRes.containsKey(group));
                Assert.assertEquals(1, groupRes.get(group).size());
            } finally {
                count.set(-1);
            }
        }, "main-sub").start();
        while (count.get() >= 0) { }
        count.set(-1);
    }


    /**
     * 测试 ThreadGroupUtil.groupThread 方法
     *  执行线程，父线程绑定TaskThreadGroup后启动后
     *    通过启动新新线程再启动ProcessorOnTaskThreadGroup绑定的子线程, 根据子ThreadGroup来找到父子关系, 符合预期情况
     * */
    @Test
    public void testGroupThreadGroup1() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = mockTaskThreadGroup();
        new Thread(group, () -> {
            try {
                ThreadGroup groupSub = mockProcessorOnTaskThreadGroup();
                new Thread(() -> {
                    count.incrementAndGet();
                    new Thread(groupSub, () -> {
                        count.incrementAndGet();
                        while (count.get() >= 0) { }
                    }, "main-sub-1-1").start();
                    while (count.get() >= 0) { }
                }, "main-sub-1").start();

                Map<ThreadGroup, Set<ThreadGroup>> groupRes = util.group(groupSub);
                Assert.assertFalse(groupRes.isEmpty());
                Assert.assertTrue(groupRes.containsKey(group));
                Assert.assertEquals(1, groupRes.get(group).size());
            } finally {
                count.set(-1);
            }
        }, "main-sub").start();
        while (count.get() >= 0) { }
        count.set(-1);
    }

    /**
     * 测试 ThreadGroupUtil.groupThread 方法,
     * 边界条件
     *  执行线程，父线程绑定TaskThreadGroup后启动后
     *     根据子ThreadGroup来找到父子关系, 使用未绑定线程的ThreadGroup来获取其ThreadGroup的父子关系，预期是获取不到任务关系的
     * */
    @Test
    public void testGroupThreadGroup2() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = mockTaskThreadGroup();
        ThreadGroup groupSub = mockProcessorOnTaskThreadGroup();
        new Thread(group, () -> {
            Map<ThreadGroup, Set<ThreadGroup>> groupRes = util.group(groupSub);
            Assert.assertTrue(groupRes.isEmpty());
        }, "main-sub").start();
    }


    /**
     * 测试 ThreadGroupUtil.containsInSubClass 方法
     *  验证ProcessorOnTaskThreadGroup, 符合预期情况
     * */
    @Test
    public void testContainsInSubClass1() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        Object containsInSubClass = invokerPrivateMethod(
                util,
                "containsInSubClass",
                getArray((Class<?>) ThreadGroup.class),
                mockProcessorOnTaskThreadGroup());
        Assert.assertEquals(Boolean.TRUE, containsInSubClass);
    }

    /**
     *
     * 测试 ThreadGroupUtil.containsInSubClass 方法
     *  验证 DisposableThreadGroup, 符合预期情况
     * */
    @Test
    public void testContainsInSubClass2() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        ThreadGroup groupSub = new DisposableThreadGroup("type", "name");
        Object containsInSubClass = invokerPrivateMethod(
                util,
                "containsInSubClass",
                getArray((Class<?>)ThreadGroup.class),
                groupSub);
        Assert.assertEquals(Boolean.TRUE, containsInSubClass);
    }

    /**
     *
     * 测试 ThreadGroupUtil.containsInSubClass 方法
     *  验证 ConnectorOnTaskThreadGroup, 符合预期情况
     * */
    @Test
    public void testContainsInSubClass3() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        ThreadGroup groupSub = mockConnectorOnTaskThreadGroup();
        Object containsInSubClass = invokerPrivateMethod(
                util,
                "containsInSubClass",
                getArray((Class<?>)ThreadGroup.class),
                groupSub);
        Assert.assertEquals(Boolean.TRUE, containsInSubClass);
    }


    /**
     * 测试 ThreadGroupUtil.containsInSubClass 方法
     *  验证ProcessorOnTaskThreadGroup，边界状态：ThreadGroup 为 null
     * */
    @Test
    public void testContainsInSubClass4() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        ThreadGroup groupSub = null;
        Object containsInSubClass = invokerPrivateMethod(
                util,
                "containsInSubClass",
                getArray((Class<?>)ThreadGroup.class),
                groupSub);
        Assert.assertEquals(Boolean.FALSE, containsInSubClass);
    }

    /**
     * 测试 ThreadGroupUtil.containsInSubClass 方法
     *  验证ProcessorOnTaskThreadGroup，边界状态：ThreadGroup 为 其他ThreadGroup类型
     * */
    @Test
    public void testContainsInSubClass5() {
        ThreadGroupUtil util = ThreadGroupUtil.THREAD_GROUP_TASK;
        ThreadGroup groupSub = mockTaskThreadGroup();
        Object containsInSubClass = invokerPrivateMethod(
                util,
                "containsInSubClass",
                getArray((Class<?>)ThreadGroup.class),
                groupSub);
        Assert.assertEquals(Boolean.FALSE, containsInSubClass);
    }




    public Object invokerPrivateMethod(Object clazzObj, String methodName, Class<?>[] paramsTypes, Object... params){
        try {
            Method privateMethod = clazzObj.getClass().getDeclaredMethod(methodName, paramsTypes);
            privateMethod.setAccessible(true);
            return privateMethod.invoke(clazzObj, params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T>T[] getArray(T ... classes) {
        return classes;
    }

    public ConnectorOnTaskThreadGroup mockConnectorOnTaskThreadGroup(){
        return new ConnectorOnTaskThreadGroup(mockDataProcessorContext());
    }

    public ProcessorOnTaskThreadGroup mockProcessorOnTaskThreadGroup() {
        return new ProcessorOnTaskThreadGroup(mockDataProcessorContext());
    }

    private DataProcessorContext mockDataProcessorContext() {
        DatabaseNode node = new DatabaseNode();
        node.setName("test-node");
        return new DataProcessorContext.DataProcessorContextBuilder().withNode(node).build();
    }

    private TaskThreadGroup mockTaskThreadGroup() {
        TaskDto taskDto = new TaskDto();
        taskDto.setName("task-test");
        return new TaskThreadGroup(taskDto);
    }
}
