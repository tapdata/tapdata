package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateDateProcessorNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.utils.MongoUtils;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DateNodeServiceImplTest {

    @Mock
    private DataSourceService dataSourceService;
    @Mock
    private DataSourceDefinitionService mockDefinitionService;
    @InjectMocks
    private DateNodeServiceImpl dateNodeServiceImplUnderTest;


    @Test
    void testCheckTaskDateNode() {
        final TaskDto taskDto = new TaskDto();
        taskDto.setSyncType("migrate");
        DatabaseNode databaseNode = new DatabaseNode();
        databaseNode.setId("test");
        databaseNode.setConnectionId("6583b4915081ca1b3cddc4f3");
        MigrateDateProcessorNode migrateDateProcessorNode = mock(MigrateDateProcessorNode.class);
        List<Node> nodeList = Arrays.asList(databaseNode,migrateDateProcessorNode);
        List<Edge> edges = Arrays.asList();
        Dag dag = new Dag();
        dag.setNodes(nodeList);
        dag.setEdges(edges);
        final DAG mockDag = DAG.build(dag);
        taskDto.setDag(mockDag);
        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        final DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
        dataSourceConnectionDto.setName("name");
        dataSourceConnectionDto.setConfig(new HashMap<>());
        dataSourceConnectionDto.setCreateType(CreateTypeEnum.User);
        dataSourceConnectionDto.setConnection_type("connection_type");
        dataSourceConnectionDto.setDatabase_type("database_type");
        when(dataSourceService.findById(any(ObjectId.class), any(Field.class), any(UserDetail.class))).thenAnswer(invocationOnMock -> {
             ObjectId connectionId = invocationOnMock.getArgument(0);
             ObjectId except = MongoUtils.toObjectId("6583b4915081ca1b3cddc4f3");
            assertEquals(except,connectionId);
            return null;
        });
        dateNodeServiceImplUnderTest.checkTaskDateNode(taskDto, userDetail);
    }
    @Test
    void testCheckTaskDateNode_DatabaseNodeServiceReturnsNull() {
        final TaskDto taskDto = new TaskDto();
        taskDto.setSyncType("migrate");
        MigrateDateProcessorNode migrateDateProcessorNode = mock(MigrateDateProcessorNode.class);
        List<Node> nodeList = Arrays.asList(migrateDateProcessorNode);
        List<Edge> edges = Arrays.asList();
        Dag dag = new Dag();
        dag.setNodes(nodeList);
        dag.setEdges(edges);
        final DAG mockDag = DAG.build(dag);
        taskDto.setDag(mockDag);
        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        dateNodeServiceImplUnderTest.checkTaskDateNode(taskDto, userDetail);
        verify(dataSourceService,times(0)).findById(any(ObjectId.class), any(Field.class), any(UserDetail.class));
    }
}
