package io.tapdata.threadgroup.utils;

import io.tapdata.threadgroup.utils.entity.MockFatherThreadGroup;
import io.tapdata.threadgroup.utils.entity.MockSubThreadGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadGroupUtilTest {
    ThreadGroupUtil util;
    @Before
    public void init() {
        util = ThreadGroupUtil.create(MockFatherThreadGroup.class, new Class[]{MockSubThreadGroup.class});
    }
    /**
     * 测试 ThreadGroupUtil.getFatherThreadGroup 方法
     *  根据子ThreadGroup获取父ThreadGroup
     * */
    @Test
    public void testGetFatherThreadGroup() {
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = new MockFatherThreadGroup();
        new Thread(group, () -> {
            try {
                ThreadGroup groupSub = new MockSubThreadGroup();
                Object fatherThreadGroup = invokerPrivateMethod(
                        util,
                        "getFatherThreadGroup",
                        getArray((Class<?>)ThreadGroup.class),
                        groupSub);
                Assert.assertNotNull(fatherThreadGroup);
                Assert.assertEquals(MockFatherThreadGroup.class, fatherThreadGroup.getClass());
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
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = new MockFatherThreadGroup();
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
     *  执行线程，父线程绑定MockFatherThreadGroup后启动后再启动MockSubThreadGroup绑定的子线程, 根据子ThreadGroup来找到父子关系, 符合预期情况
     * */
    @Test
    public void testGroupThreadGroup0() {
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = new MockFatherThreadGroup();
        new Thread(group, () -> {
            try {
                ThreadGroup groupSub = new MockSubThreadGroup();
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
     *  执行线程，父线程绑定MockFatherThreadGroup后启动后
     *    通过启动新新线程再启动MockSubThreadGroup绑定的子线程, 根据子ThreadGroup来找到父子关系, 符合预期情况
     * */
    @Test
    public void testGroupThreadGroup1() {
        AtomicInteger count = new AtomicInteger(0);
        ThreadGroup group = new MockFatherThreadGroup();
        new Thread(group, () -> {
            try {
                ThreadGroup groupSub = new MockSubThreadGroup();
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
     *  执行线程，父线程绑定MockFatherThreadGroup后启动后
     *     根据子ThreadGroup来找到父子关系, 使用未绑定线程的ThreadGroup来获取其ThreadGroup的父子关系，预期是获取不到任务关系的
     * */
    @Test
    public void testGroupThreadGroup2() {
        ThreadGroup group = new MockFatherThreadGroup();
        ThreadGroup groupSub = new MockSubThreadGroup();
        new Thread(group, () -> {
            Map<ThreadGroup, Set<ThreadGroup>> groupRes = util.group(groupSub);
            Assert.assertTrue(groupRes.isEmpty());
        }, "main-sub").start();
    }

    /**
     *
     * 测试 ThreadGroupUtil.containsInSubClass 方法
     *  验证 MockSubThreadGroup, 符合预期情况
     * */
    @Test
    public void testContainsInSubClass3() {
        ThreadGroup groupSub = new MockSubThreadGroup();
        Object containsInSubClass = invokerPrivateMethod(
                util,
                "containsInSubClass",
                getArray((Class<?>)ThreadGroup.class),
                groupSub);
        Assert.assertEquals(Boolean.TRUE, containsInSubClass);
    }


    /**
     * 测试 ThreadGroupUtil.containsInSubClass 方法
     *  验证MockSubThreadGroup，边界状态：ThreadGroup 为 null
     * */
    @Test
    public void testContainsInSubClass4() {
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
     *  验证MockFatherThreadGroup，边界状态：ThreadGroup 为 其他ThreadGroup类型
     * */
    @Test
    public void testContainsInSubClass5() {
        ThreadGroup groupSub = new MockFatherThreadGroup();
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
}
