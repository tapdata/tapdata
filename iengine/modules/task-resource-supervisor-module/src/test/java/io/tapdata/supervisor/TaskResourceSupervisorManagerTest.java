package io.tapdata.supervisor;

import com.tapdata.tm.commons.dag.Node;
import io.tapdata.entity.script.ScriptFactory;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TaskResourceSupervisorManagerTest {

    @Nested
    class CleanThreadGroupTest{

        private TaskResourceSupervisorManager taskResourceSupervisorManager;
        @DisplayName("test cleanThreadGroup when destroy success ")
        @Test
        @SneakyThrows
        void test1(){
            ThreadGroup threadGroup=new ThreadGroup("testThreadGroup");
            SupervisorAspectTask supervisorAspectTask=new SupervisorAspectTask();
            Map<ThreadGroup, TaskNodeInfo> threadGroupMap = new ConcurrentHashMap<>();

            TaskNodeInfo taskNodeInfo=new TaskNodeInfo();
            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(threadGroup);
            threadGroupMap.put(threadGroup,taskNodeInfo);
            ReflectionTestUtils.setField(supervisorAspectTask,"threadGroupMap",threadGroupMap);
            taskNodeInfo.setSupervisorAspectTask(supervisorAspectTask);

            taskResourceSupervisorManager = new TaskResourceSupervisorManager();
            reflectField("0");
            taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
            taskResourceSupervisorManager.cleanThreadGroup();
            assertEquals(Boolean.FALSE,taskNodeInfo.isHasLaked());
        }
        @DisplayName("test cleanThreadGroup when destroy failed ")
        @Test
        @SneakyThrows
        void test2(){
            ThreadGroup spyThreadGroup = new ThreadGroup("123");
            Thread[] threads = new Thread[1];
            AtomicBoolean atomicBoolean=new AtomicBoolean(false);
            threads[0] = new Thread(spyThreadGroup, () -> {
                while (!atomicBoolean.get()) {
                }
            });
            Node node = mock(Node.class);
            when(node.getId()).thenReturn("testId");

            TaskNodeInfo taskNodeInfo=new TaskNodeInfo();
            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(spyThreadGroup);

            taskResourceSupervisorManager = new TaskResourceSupervisorManager();
            reflectField("0");
            taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
            taskResourceSupervisorManager.cleanThreadGroup();
            atomicBoolean.set(true);
            assertEquals(Boolean.TRUE,taskNodeInfo.isHasLaked());
        }
        private void reflectField(String threshold) throws NoSuchFieldException, IllegalAccessException {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Unsafe unsafe = (Unsafe) theUnsafe.get(null);

            Field field = TaskResourceSupervisorManager.class.getDeclaredField("CLEAN_LEAKED_THREAD_GROUP_THRESHOLD");

            Object fieldBase = unsafe.staticFieldBase(field);
            long fieldOffset = unsafe.staticFieldOffset(field);

            unsafe.putObject(fieldBase, fieldOffset, "0");
        }
    }
    @Nested
    class TestSummary{
        Set<SupervisorAspectTask> aliveSet;
        Set<SupervisorAspectTask> leakedSet;
        TaskNodeInfo taskNodeInfo;
        TaskResourceSupervisorManager taskResourceSupervisorManager;
        @BeforeEach
        void init(){
            aliveSet=new HashSet<>();
            leakedSet=new HashSet<>();
            taskNodeInfo=new TaskNodeInfo();
            taskResourceSupervisorManager=new TaskResourceSupervisorManager();
        }
        @DisplayName("test summary when task is stop ,and thread group destroy success")
        @Test
        void test1(){
            SupervisorAspectTask supervisorAspectTask = new SupervisorAspectTask();
            ThreadGroup threadGroup=new ThreadGroup("test");
            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(threadGroup);
            taskNodeInfo.setSupervisorAspectTask(supervisorAspectTask);
            Set<TaskNodeInfo> summarySet =new HashSet<>();
            summarySet.add(taskNodeInfo);
            taskResourceSupervisorManager.summary(summarySet,aliveSet,leakedSet);
            assertEquals(Boolean.FALSE,taskNodeInfo.isHasLaked());
        }
        @DisplayName("test summary when task is alive")
        @Test
        void test2(){
            ThreadGroup threadGroup=new ThreadGroup("test");
            taskNodeInfo.setHasLeaked(false);
            taskNodeInfo.setNodeThreadGroup(threadGroup);
            taskNodeInfo.setSupervisorAspectTask(new SupervisorAspectTask());
            Set<TaskNodeInfo> summarySet =new HashSet<>();
            summarySet.add(taskNodeInfo);
            taskResourceSupervisorManager.summary(summarySet,aliveSet,leakedSet);
            assertEquals(1,aliveSet.size());
        }
        @DisplayName("test summary when task is stop,thread group destroy failed lead to leaked")
        @Test
        void test3(){
            ThreadGroup spyThreadGroup = new ThreadGroup("123");
            Thread[] threads = new Thread[1];
            AtomicBoolean atomicBoolean=new AtomicBoolean(false);
            threads[0] = new Thread(spyThreadGroup, () -> {
                while (!atomicBoolean.get()) {
                }
            });
            threads[0].start();
            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(spyThreadGroup);
            taskNodeInfo.setSupervisorAspectTask(new SupervisorAspectTask());
            Set<TaskNodeInfo> summarySet =new HashSet<>();
            summarySet.add(taskNodeInfo);
            taskResourceSupervisorManager.summary(summarySet,aliveSet,leakedSet);
            atomicBoolean.set(true);
            assertEquals(1,leakedSet.size());
        }
    }
}
