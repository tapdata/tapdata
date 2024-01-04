package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

class TransformSchemaServiceTest {
    private TransformSchemaService transformSchemaService;
    private DataSourceService dataSourceService;
    private MetadataInstancesService metadataInstancesService;
    private MetadataTransformerService metadataTransformerService;
    private DataSourceDefinitionService definitionService;

    @BeforeEach
    void buildTransformSchemaService(){
        transformSchemaService = mock(TransformSchemaService.class);
        dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(transformSchemaService,"dataSourceService",dataSourceService);
        metadataInstancesService = mock(MetadataInstancesService.class);
        ReflectionTestUtils.setField(transformSchemaService,"metadataInstancesService",metadataInstancesService);
        metadataTransformerService = mock(MetadataTransformerService.class);
        ReflectionTestUtils.setField(transformSchemaService,"metadataTransformerService",metadataTransformerService);
        definitionService = mock(DataSourceDefinitionService.class);
        ReflectionTestUtils.setField(transformSchemaService,"definitionService",definitionService);
    }
    @Nested
    class getTransformParamTest{
        @Test
        void testGetTransformParamSimple(){
            TaskDto taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            UserDetail user = mock(UserDetail.class);
            List<String> includes = new ArrayList<>();
            boolean allParam = true;
            doNothing().when(dag).addNodeEventListener(any());
            doCallRealMethod().when(transformSchemaService).getTransformParam(taskDto,user,includes,allParam);
            TransformerWsMessageDto actual = transformSchemaService.getTransformParam(taskDto, user, includes, allParam);
            verify(metadataTransformerService,new Times(1)).findAllDto(any(),any());
        }
        @Test
        void testGetTransformParamWithTableNode(){
            TaskDto taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            UserDetail user = mock(UserDetail.class);
            List<String> includes = new ArrayList<>();
            boolean allParam = false;
            doNothing().when(dag).addNodeEventListener(any());
            List<Node> dagNodes = new ArrayList<>();
            Node node1 = new TableNode();
            List<DataSourceConnectionDto> dataSources = new ArrayList<>();
            DataSourceConnectionDto dataSourceConnectionDto = mock(DataSourceConnectionDto.class);
            when(dataSourceConnectionDto.getId()).thenReturn(new ObjectId());
            when(dataSourceConnectionDto.getDatabase_type()).thenReturn("mongodb");
            List<DataSourceDefinitionDto> definitionDtos = new ArrayList<>();
            DataSourceDefinitionDto dataSourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            definitionDtos.add(dataSourceDefinitionDto);
            when(dataSourceDefinitionDto.getType()).thenReturn("mongodb");
            when(definitionService.getByDataSourceType(anyList(),any())).thenReturn(definitionDtos);
            when(metadataInstancesService.getQualifiedNameByNodeId(node1, user, dataSourceConnectionDto, dataSourceDefinitionDto, taskDto.getId().toHexString())).thenReturn("qualified_name");
            dataSources.add(dataSourceConnectionDto);
            Map<String, DataSourceConnectionDto> dataSourceMap = dataSources.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), d -> d, (d1, d2) -> d1));
            node1.setDag(mock(DAG.class));
            for (String s : dataSourceMap.keySet()) {
                ((TableNode)node1).setConnectionId(s);
            }
            dagNodes.add(node1);
            List<String> connectionIds = dagNodes.stream().filter(n -> n instanceof DataParentNode).map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toList());
            Criteria idCriteria = Criteria.where("_id").in(connectionIds);
            Query query = new Query(idCriteria);
            when(dataSourceService.findAll(query)).thenReturn(dataSources);
            when(dag.getNodes()).thenReturn(dagNodes);
            doCallRealMethod().when(transformSchemaService).getTransformParam(taskDto,user,includes,allParam);
            transformSchemaService.getTransformParam(taskDto, user, includes, allParam);
            verify(metadataTransformerService,new Times(1)).findAllDto(any(),any());
        }
        @Test
        void testGetTransformParamWithDatabaseNode(){
            TaskDto taskDto = new TaskDto();
            DAG dag = mock(DAG.class);
            taskDto.setDag(dag);
            taskDto.setId(new ObjectId());
            UserDetail user = mock(UserDetail.class);
            List<String> includes = new ArrayList<>();
            boolean allParam = false;
            doNothing().when(dag).addNodeEventListener(any());
            List<Node> dagNodes = new ArrayList<>();
            Node node1 = new DatabaseNode();
            List<DataSourceConnectionDto> dataSources = new ArrayList<>();
            DataSourceConnectionDto dataSourceConnectionDto = mock(DataSourceConnectionDto.class);
            when(dataSourceConnectionDto.getId()).thenReturn(new ObjectId());
            when(dataSourceConnectionDto.getDatabase_type()).thenReturn("mongodb");
            List<DataSourceDefinitionDto> definitionDtos = new ArrayList<>();
            DataSourceDefinitionDto dataSourceDefinitionDto = mock(DataSourceDefinitionDto.class);
            definitionDtos.add(dataSourceDefinitionDto);
            when(dataSourceDefinitionDto.getType()).thenReturn("mongodb");
            when(definitionService.getByDataSourceType(anyList(),any())).thenReturn(definitionDtos);
            when(metadataInstancesService.getQualifiedNameByNodeId(node1, user, dataSourceConnectionDto, dataSourceDefinitionDto, taskDto.getId().toHexString())).thenReturn("qualified_name");
            dataSources.add(dataSourceConnectionDto);
            Map<String, DataSourceConnectionDto> dataSourceMap = dataSources.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), d -> d, (d1, d2) -> d1));
            node1.setDag(mock(DAG.class));
            for (String s : dataSourceMap.keySet()) {
                ((DatabaseNode)node1).setConnectionId(s);
            }
            dagNodes.add(node1);
            List<String> connectionIds = dagNodes.stream().filter(n -> n instanceof DataParentNode).map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toList());
            Criteria idCriteria = Criteria.where("_id").in(connectionIds);
            Query query = new Query(idCriteria);
            when(dataSourceService.findAll(query)).thenReturn(dataSources);
            when(dag.getNodes()).thenReturn(dagNodes);
            doCallRealMethod().when(transformSchemaService).getTransformParam(taskDto,user,includes,allParam);
            transformSchemaService.getTransformParam(taskDto, user, includes, allParam);
            verify(metadataTransformerService,new Times(1)).findAllDto(any(),any());
        }
    }
}