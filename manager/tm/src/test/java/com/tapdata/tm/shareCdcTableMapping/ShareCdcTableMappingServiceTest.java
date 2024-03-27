package com.tapdata.tm.shareCdcTableMapping;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.shareCdcTableMapping.repository.ShareCdcTableMappingRepository;
import com.tapdata.tm.shareCdcTableMapping.service.ShareCdcTableMappingService;
import com.tapdata.tm.shareCdcTableMapping.service.ShareCdcTableMappingServiceImpl;
import com.tapdata.tm.task.service.TaskService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

 class ShareCdcTableMappingServiceTest {

    @Test
     void genShareCdcTableMappingsByLogCollectorTaskAddUserTest() {
        ShareCdcTableMappingRepository shareCdcTableMappingRepository = Mockito.mock(ShareCdcTableMappingRepository.class);

        ShareCdcTableMappingService shareCdcTableMappingService = new ShareCdcTableMappingServiceImpl(shareCdcTableMappingRepository);
        TaskDto logCollectorTask = new TaskDto();
        logCollectorTask.setId(new ObjectId("62bc500bd4958d013d97e253"));
        Dag dag = new Dag();
        LogCollectorNode logCollectorNode = new LogCollectorNode();
        List tableName = new ArrayList();
        tableName.add("test");
        logCollectorNode.setTableNames(tableName);
        List connectionIds = new ArrayList();
        connectionIds.add("62bc500bd4958d013d97e254");
        logCollectorNode.setConnectionIds(connectionIds);
        List<Node> nodes = new ArrayList<>();
        nodes.add(logCollectorNode);
        dag.setNodes(nodes);
        dag.setEdges(new ArrayList<>());
        logCollectorTask.setDag(DAG.build(dag));
        UserDetail user = Mockito.mock(UserDetail.class);
        TaskService taskService = Mockito.mock(TaskService.class);
        ReflectionTestUtils.setField(shareCdcTableMappingService, "taskService", taskService);
        DataSourceService dataSourceService = Mockito.mock(DataSourceService.class);
        ReflectionTestUtils.setField(shareCdcTableMappingService, "dataSourceService", dataSourceService);
        BulkOperations bulkOperations = Mockito.mock(BulkOperations.class);
        when(shareCdcTableMappingRepository.bulkOperations(BulkOperations.BulkMode.UNORDERED)).thenReturn(bulkOperations);
        when(shareCdcTableMappingRepository.buildUpdateSet(org.mockito.Matchers.any(), org.mockito.Matchers.any())).thenAnswer(invocationOnMock -> {
            UserDetail actualData = invocationOnMock.getArgument(1, UserDetail.class);
            UserDetail exceptData = user;
            assertEquals(exceptData, actualData);
            return null;
        });
        shareCdcTableMappingService.genShareCdcTableMappingsByLogCollectorTask(logCollectorTask, false, user);


    }
}
