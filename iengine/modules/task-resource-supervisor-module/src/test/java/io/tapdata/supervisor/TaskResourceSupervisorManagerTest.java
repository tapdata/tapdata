package io.tapdata.supervisor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TaskResourceSupervisorManagerTest {
    @Nested
    class CleanThreadGroupTest{
        @DisplayName("test cleanThreadGroup when destroy success ")
        @Test
        void test1(){
            TaskNodeInfo taskNodeInfo=new TaskNodeInfo();
            taskNodeInfo.setHasLeaked(true);
            ThreadGroup threadGroup = mock(ThreadGroup.class);
            taskNodeInfo.setNodeThreadGroup(threadGroup);

            TaskResourceSupervisorManager taskResourceSupervisorManager = new TaskResourceSupervisorManager();

            ReflectionTestUtils.setField(taskResourceSupervisorManager,"CLEAN_LEAKED_THREAD_GROUP_THRESHOLD","0");
            taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
            taskResourceSupervisorManager.cleanThreadGroup();
            assertEquals(Boolean.FALSE,taskNodeInfo.isHasLaked());
        }
        @DisplayName("test cleanThreadGroup when destroy faild ")
        @Test
        void test2(){
            TaskNodeInfo taskNodeInfo=new TaskNodeInfo();
            taskNodeInfo.setHasLeaked(true);
            ThreadGroup threadGroup=new ThreadGroup("testThread");
            ThreadGroup spyThreadGroup = spy(threadGroup);
            doThrow(IllegalThreadStateException.class).when(spyThreadGroup).destroy();
            taskNodeInfo.setNodeThreadGroup(spyThreadGroup);

            TaskResourceSupervisorManager taskResourceSupervisorManager = new TaskResourceSupervisorManager();
            ReflectionTestUtils.setField(taskResourceSupervisorManager,"CLEAN_LEAKED_THREAD_GROUP_THRESHOLD","0");
            taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);
            taskResourceSupervisorManager.cleanThreadGroup();
            assertEquals(Boolean.TRUE,taskNodeInfo.isHasLaked());
        }
    }
}
