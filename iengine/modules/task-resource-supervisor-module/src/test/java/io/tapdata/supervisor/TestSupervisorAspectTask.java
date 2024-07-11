package io.tapdata.supervisor;

import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.supervisor.DataNodeThreadGroupAspect;
import io.tapdata.aspect.supervisor.ThreadGroupAspect;
import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TestSupervisorAspectTask {
    private String associateId;
    private SupervisorAspectTask supervisorAspectTask;
    TaskResourceSupervisorManager taskResourceSupervisorManager;
    DataProcessorContext dataProcessorContext;
    Node mockNode;
    @BeforeEach
    void setUp(){
        associateId = "associateId";
        supervisorAspectTask=new SupervisorAspectTask();
        taskResourceSupervisorManager=new TaskResourceSupervisorManager();
        mockNode = mock(Node.class);
        when(mockNode.getId()).thenReturn("testNodeId");
        dataProcessorContext = mock(DataProcessorContext.class);
        when(dataProcessorContext.getNode()).thenReturn(mockNode);
    }


    @DisplayName("test method addAspect reuse taskNodeInfo")
    @Test
    void test1(){

        Node mockNode = mock(Node.class);

        DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
        when(dataProcessorContext.getNode()).thenReturn(mockNode);
        when(mockNode.getId()).thenReturn("testNodeId");
        when(mockNode.getName()).thenReturn("testName");

        ConnectorOnTaskThreadGroup connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);
        TaskNodeInfo taskNodeInfo = new TaskNodeInfo();
        taskNodeInfo.setNode(mockNode);
        taskNodeInfo.setSupervisorAspectTask(supervisorAspectTask);
        taskNodeInfo.setAssociateId(associateId);
        taskNodeInfo.setNodeThreadGroup(connectorOnTaskThreadGroup);

        taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);

        ReflectionTestUtils.setField(supervisorAspectTask, "taskResourceSupervisorManager", taskResourceSupervisorManager);


        ThreadGroupAspect aspect=new DataNodeThreadGroupAspect(mockNode,associateId,connectorOnTaskThreadGroup);

        supervisorAspectTask.addAspect(aspect);
        assertEquals(1,supervisorAspectTask.getThreadGroupMap().size());
        assertEquals(taskNodeInfo,supervisorAspectTask.getThreadGroupMap().get(connectorOnTaskThreadGroup));
    }
    @DisplayName("test method addAspect not reuse taskNodeInfo")
    @Test
    void test2(){


        Node mockNode = mock(Node.class);
        when(mockNode.getId()).thenReturn("testNodeId");

        DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
        when(dataProcessorContext.getNode()).thenReturn(mockNode);

        ConnectorOnTaskThreadGroup connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);
        ReflectionTestUtils.setField(supervisorAspectTask, "taskResourceSupervisorManager", taskResourceSupervisorManager);

        String associateId = "associateId";
        ThreadGroupAspect aspect=new DataNodeThreadGroupAspect(mockNode,associateId,connectorOnTaskThreadGroup);
        supervisorAspectTask.addAspect(aspect);
        assertEquals(1,taskResourceSupervisorManager.getTaskNodeInfos().size());

    }
    @DisplayName("test on stop release threadGroup success")
    @Test
    void test3(){
        ReflectionTestUtils.setField(supervisorAspectTask, "taskResourceSupervisorManager", taskResourceSupervisorManager);

        Node mockNode = mock(Node.class);
        when(mockNode.getId()).thenReturn("testNodeId");
        DataProcessorContext dataProcessorContext = mock(DataProcessorContext.class);
        when(dataProcessorContext.getNode()).thenReturn(mockNode);
        ConnectorOnTaskThreadGroup connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);

        TaskNodeInfo taskNodeInfo = new TaskNodeInfo();
        taskNodeInfo.setNode(mockNode);
        taskNodeInfo.setSupervisorAspectTask(supervisorAspectTask);
        taskNodeInfo.setAssociateId(associateId);
        taskNodeInfo.setNodeThreadGroup(connectorOnTaskThreadGroup);
        taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);

        Map<ThreadGroup, TaskNodeInfo> threadGroupMap = new ConcurrentHashMap<>();
        threadGroupMap.put(connectorOnTaskThreadGroup,taskNodeInfo);

        ReflectionTestUtils.setField(supervisorAspectTask, "threadGroupMap", threadGroupMap);
        TaskStopAspect taskStopAspect=new TaskStopAspect();
        supervisorAspectTask.onStop(taskStopAspect);
        assertEquals(Boolean.FALSE,taskNodeInfo.isHasLaked());
    }
    @DisplayName("test on stop release threadGroup failed")
    @Test
    void test4(){

        ReflectionTestUtils.setField(supervisorAspectTask, "taskResourceSupervisorManager", taskResourceSupervisorManager);

        ConnectorOnTaskThreadGroup connectorOnTaskThreadGroup = new ConnectorOnTaskThreadGroup(dataProcessorContext);
        ConnectorOnTaskThreadGroup spyThreadGroup = spy(connectorOnTaskThreadGroup);
        doThrow(IllegalThreadStateException.class).when(spyThreadGroup).destroy();
        TaskNodeInfo taskNodeInfo = new TaskNodeInfo();
        taskNodeInfo.setNode(mockNode);
        taskNodeInfo.setSupervisorAspectTask(supervisorAspectTask);
        taskNodeInfo.setAssociateId(associateId);
        taskNodeInfo.setNodeThreadGroup(spyThreadGroup);

        taskResourceSupervisorManager.addTaskSubscribeInfo(taskNodeInfo);

        Map<ThreadGroup, TaskNodeInfo> threadGroupMap = new ConcurrentHashMap<>();
        threadGroupMap.put(spyThreadGroup,taskNodeInfo);
        ReflectionTestUtils.setField(supervisorAspectTask, "threadGroupMap", threadGroupMap);

        TaskStopAspect taskStopAspect=new TaskStopAspect();
        supervisorAspectTask.onStop(taskStopAspect);
        assertEquals(Boolean.TRUE,taskNodeInfo.isHasLaked());
        assertEquals(1,taskResourceSupervisorManager.getTaskNodeInfos().size());
    }
}
