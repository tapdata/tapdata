package com.tapdata.pdk;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.utils.UnitTestUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/13 15:25 Create
 */
class TaskPdkConnectorTest {

    @Mock
    ClientMongoOperator clientMongoOperator;
    @Mock
    TaskDto taskDto;
    @Mock
    DAG dag;
    @Mock
    DataParentNode<?> sourceNode;
    @Mock
    DataParentNode<?> targetNode;
    @Mock
    Connections connections;
    @Mock
    TaskPdkConnector taskPdkConnector;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(taskDto.getId()).thenReturn(new ObjectId("507f1f77bcf86cd799439011"));
        when(taskDto.getDag()).thenReturn(dag);
        UnitTestUtils.injectField(TaskPdkConnector.class, taskPdkConnector, "task", taskDto);
        UnitTestUtils.injectField(TaskPdkConnector.class, taskPdkConnector, "taskId", taskDto.getId().toHexString());
        UnitTestUtils.injectField(TaskPdkConnector.class, taskPdkConnector, "clientMongoOperator", clientMongoOperator);

        when(dag.getSources()).thenReturn(Collections.singletonList(sourceNode));
        when(dag.getTargets()).thenReturn(Collections.singletonList(targetNode));
        when(sourceNode.getConnectionId()).thenReturn("sourceConnectionId");
        when(targetNode.getConnectionId()).thenReturn("targetConnectionId");
        when(connections.getPdkHash()).thenReturn("pdkHash");
    }

    @Test
    void testConstructor() {
        try (MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)) {
            beanUtilMockedStatic.when(() -> BeanUtil.getBean(ClientMongoOperator.class)).thenReturn(clientMongoOperator);
            TaskPdkConnector pdkConnector = new TaskPdkConnector(taskDto);
            assertNotNull(pdkConnector);
            assertNotNull(TaskPdkConnector.of(taskDto));
        }
    }

    @Test
    void testCreateSource() {
        // Arrange
        doCallRealMethod().when(taskPdkConnector).createSource(anyString());
        IPdkConnector expectPdkConnector = mock(IPdkConnector.class);
        doReturn(expectPdkConnector).when(taskPdkConnector).createFirstMatch(anyString(), eq(true));

        // Act
        IPdkConnector connector = taskPdkConnector.createSource("associateId");

        // Assert
        assertNotNull(connector);
        assertEquals(expectPdkConnector, connector);
    }

    @Test
    void testCreateTarget() {
        // Arrange
        doCallRealMethod().when(taskPdkConnector).createTarget(anyString());
        IPdkConnector expectPdkConnector = mock(IPdkConnector.class);
        doReturn(expectPdkConnector).when(taskPdkConnector).createFirstMatch(anyString(), eq(false));

        // Act
        IPdkConnector connector = taskPdkConnector.createTarget("associateId");

        // Assert
        assertNotNull(connector);
        assertEquals(expectPdkConnector, connector);
    }

    @Nested
    class CreateTest {

        @Test
        void testById() {
            // Arrange
            String nodeId = "existNodeId";
            doCallRealMethod().when(taskPdkConnector).create(anyString(), anyString());
            when(dag.getNode(nodeId)).thenReturn((Node) sourceNode);

            // Act
            IPdkConnector connector = taskPdkConnector.create(nodeId, "associateId");

            // Assert
            assertNull(connector);
            verify(taskPdkConnector, times(1)).create(eq(sourceNode), anyString());
        }

        @Test
        void testByNode_Null() {
            // Arrange
            doCallRealMethod().when(taskPdkConnector).create(any(Node.class), anyString());

            // Act
            IPdkConnector connector = taskPdkConnector.create((Node) null, "associateId");

            // Assert
            assertNull(connector);
            verify(taskPdkConnector, times(0)).getConnections(anyString());
        }

        @Test
        void testByNode_NullConnections() {
            // Arrange
            doCallRealMethod().when(taskPdkConnector).create(any(Node.class), anyString());
            doReturn(null).when(taskPdkConnector).getConnections(anyString());

            // Act
            IPdkConnector connector = taskPdkConnector.create(sourceNode, "associateId");

            // Assert
            assertNull(connector);
            verify(taskPdkConnector, times(1)).getConnections(anyString());
        }

        @Test
        void testByNode() {
            // Arrange
            doCallRealMethod().when(taskPdkConnector).create(any(Node.class), anyString());
            doReturn(connections).when(taskPdkConnector).getConnections(anyString());
            UnitTestUtils.injectField(TaskPdkConnector.class, taskPdkConnector, "taskConfig", mock(TaskConfig.class));

            try (
                MockedStatic<ConnectionUtil> connectionUtilMockedStatic = mockStatic(ConnectionUtil.class);
                MockedStatic<TaskNodePdkConnector> connectorMockedStatic = mockStatic(TaskNodePdkConnector.class)
            ) {
                connectionUtilMockedStatic.when(() -> ConnectionUtil.getDatabaseType(eq(clientMongoOperator), anyString()))
                    .thenReturn(null);
                TaskNodePdkConnector pdkConnector = mock(TaskNodePdkConnector.class);
                connectorMockedStatic.when(() -> TaskNodePdkConnector.create(any(), anyString(), any(), anyString(), any(), any(), any()))
                    .thenReturn(pdkConnector);


                // Act
                IPdkConnector connector = taskPdkConnector.create(sourceNode, "associateId");

                // Assert
                assertEquals(pdkConnector, connector);
                verify(taskPdkConnector, times(1)).getConnections(anyString());
            }
        }
    }

    @Nested
    class CreateFirstMatchTest {

        @Test
        public void testSource() {
            // Arrange
            String associateId = "associateId";
            doCallRealMethod().when(taskPdkConnector).createFirstMatch(eq(associateId), eq(true));
            when(dag.getSources()).thenReturn(Collections.singletonList(sourceNode));

            // Act
            IPdkConnector connector = taskPdkConnector.createFirstMatch(associateId, true);

            // Assert
            assertNull(connector);
            verify(taskPdkConnector, times(1)).create(eq(sourceNode), eq(associateId));
        }

        @Test
        public void testTarget() {
            // Arrange
            String associateId = "associateId";
            doCallRealMethod().when(taskPdkConnector).createFirstMatch(eq(associateId), eq(false));
            when(dag.getTargets()).thenReturn(Collections.singletonList(targetNode));

            // Act
            IPdkConnector connector = taskPdkConnector.createFirstMatch(associateId, false);

            // Assert
            assertNull(connector);
            verify(taskPdkConnector, times(1)).create(eq(targetNode), eq(associateId));
        }

        @Test
        public void testEmpty() {
            // Arrange
            String associateId = "associateId";
            doCallRealMethod().when(taskPdkConnector).createFirstMatch(eq(associateId), eq(false));
            when(dag.getTargets()).thenReturn(Collections.EMPTY_LIST);

            // Act
            IPdkConnector connector = taskPdkConnector.createFirstMatch(associateId, false);

            // Assert
            assertNull(connector);
            verify(taskPdkConnector, times(0)).create(eq(targetNode), eq(associateId));
        }
    }


    @Test
    public void testGetTaskConfig() {
        // Arrange
        doCallRealMethod().when(taskPdkConnector).getTaskConfig(any());
        TaskConfig taskConfig = taskPdkConnector.getTaskConfig(taskDto);

        // Assert
        assertNotNull(taskConfig);
        assertEquals(5L, taskConfig.getTaskRetryConfig().getRetryIntervalSecond());
        assertEquals(1200L, taskConfig.getTaskRetryConfig().getMaxRetryTimeSecond());
    }

    @Test
    public void testGetConnections_ConnectionExists() {
        // Arrange
        doCallRealMethod().when(taskPdkConnector).getConnections(anyString());
        when(clientMongoOperator.find(any(Query.class), eq(ConnectorConstant.CONNECTION_COLLECTION + "/listAll"), eq(Connections.class)))
            .thenReturn(Collections.singletonList(connections));

        // Act
        Connections result = taskPdkConnector.getConnections("sourceConnectionId");

        // Assert
        assertNotNull(result);
        assertEquals(connections, result);
    }

    @Test
    public void testGetConnections_ConnectionDoesNotExist() {
        // Arrange
        doCallRealMethod().when(taskPdkConnector).getConnections(anyString());
        when(clientMongoOperator.find(any(Query.class), eq(ConnectorConstant.CONNECTION_COLLECTION + "/listAll"), eq(Connections.class)))
            .thenReturn(new ArrayList<>());

        // Act
        Connections result = taskPdkConnector.getConnections("nonExistentConnectionId");

        // Assert
        assertNull(result);
    }
}
