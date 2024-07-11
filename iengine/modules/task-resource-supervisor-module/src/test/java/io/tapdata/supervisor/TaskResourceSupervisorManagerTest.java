package io.tapdata.supervisor;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TaskResourceSupervisorManagerTest {
    private static ThreadGroup spyThreadGroup;

    @BeforeAll
    static void beforeAll() {
        spyThreadGroup = new ThreadGroup("123");
        Thread[] threads = new Thread[1];
        threads[0]=new Thread(spyThreadGroup,()->{
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        threads[0].start();
    }
    @Nested
    class CleanThreadGroupTest{

        @DisplayName("test cleanThreadGroup when destroy success ")
        @Test
        void test1(){
            ThreadGroup threadGroup=mock(ThreadGroup.class);
            SupervisorAspectTask supervisorAspectTask=new SupervisorAspectTask();
            Map<ThreadGroup, TaskNodeInfo> threadGroupMap = new ConcurrentHashMap<>();

            TaskNodeInfo taskNodeInfo=new TaskNodeInfo();
            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(threadGroup);
            threadGroupMap.put(threadGroup,taskNodeInfo);
            ReflectionTestUtils.setField(supervisorAspectTask,"threadGroupMap",threadGroupMap);
            taskNodeInfo.setSupervisorAspectTask(supervisorAspectTask);

            TaskResourceSupervisorManager taskResourceSupervisorManager = new TaskResourceSupervisorManager();

            ReflectionTestUtils.setField(taskResourceSupervisorManager,"CLEAN_LEAKED_THREAD_GROUP_THRESHOLD","0");
            taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
            taskResourceSupervisorManager.cleanThreadGroup();
            assertEquals(Boolean.FALSE,taskNodeInfo.isHasLaked());
        }
        @DisplayName("test cleanThreadGroup when destroy failed ")
        @Test
        void test2(){
            Node node = mock(Node.class);
            when(node.getId()).thenReturn("testId");
            DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
            when(dataProcessorContext.getNode()).thenReturn(node);


            TaskNodeInfo taskNodeInfo=new TaskNodeInfo();
            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(spyThreadGroup);

            TaskResourceSupervisorManager taskResourceSupervisorManager = new TaskResourceSupervisorManager();
            ReflectionTestUtils.setField(taskResourceSupervisorManager,"CLEAN_LEAKED_THREAD_GROUP_THRESHOLD","0");
            taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
            taskResourceSupervisorManager.cleanThreadGroup();
            assertEquals(Boolean.TRUE,taskNodeInfo.isHasLaked());
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
            ThreadGroup threadGroup=mock(ThreadGroup.class);
            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(threadGroup);
            Set<TaskNodeInfo> summarySet =new HashSet<>();
            summarySet.add(taskNodeInfo);
            taskResourceSupervisorManager.summary(summarySet,aliveSet,leakedSet);
        }
        @DisplayName("test summary when task is alive")
        @Test
        void test2(){
            ThreadGroup threadGroup=mock(ThreadGroup.class);
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
        void test4(){

            taskNodeInfo.setHasLeaked(true);
            taskNodeInfo.setNodeThreadGroup(spyThreadGroup);

            taskNodeInfo.setSupervisorAspectTask(new SupervisorAspectTask());
            Set<TaskNodeInfo> summarySet =new HashSet<>();
            summarySet.add(taskNodeInfo);
            taskResourceSupervisorManager.summary(summarySet,aliveSet,leakedSet);
            assertEquals(1,leakedSet.size());
        }
    }
}
