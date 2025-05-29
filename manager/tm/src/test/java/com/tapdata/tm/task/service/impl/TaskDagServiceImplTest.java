package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class TaskDagServiceImplTest {
    @Nested
    class calculationDagHashTest {
        private TaskDto taskDto;

        private DAG dag;

        private DatabaseNode sourceNode;

        private TableRenameProcessNode tableRenameNode;

        private MigrateFieldRenameProcessorNode fieldRenameNode;

        private MigrateJsProcessorNode jsProcessorNode;

        private TaskDagServiceImpl taskDagServiceImpl;

        private LinkedList<DatabaseNode> sourceNodes;
        private LinkedList<Node> dagNodes;

        @BeforeEach
        public void setUp() {
            taskDto = mock(TaskDto.class);
            dag = mock(DAG.class);
            taskDagServiceImpl = spy(TaskDagServiceImpl.class);
            sourceNode = mock(DatabaseNode.class);
            tableRenameNode = mock(TableRenameProcessNode.class);
            fieldRenameNode = mock(MigrateFieldRenameProcessorNode.class);
            jsProcessorNode = mock(MigrateJsProcessorNode.class);
            sourceNodes = new LinkedList<>();
            dagNodes = new LinkedList<>();
            sourceNodes.add(sourceNode);

            when(taskDto.getDag()).thenReturn(dag);
            when(dag.getSourceNode()).thenReturn(sourceNodes);
            when(sourceNode.getTableNames()).thenReturn(Arrays.asList("table1", "table2"));
            when(dag.getNodes()).thenReturn(dagNodes);
        }

        @Test
        public void testCalculationDagHash_Success() {
            LinkedHashSet<TableRenameTableInfo> renameInfos = new LinkedHashSet<>();
            renameInfos.add(mock(TableRenameTableInfo.class));
            when(tableRenameNode.getTableNames()).thenReturn(renameInfos);
            dagNodes.add(tableRenameNode);

            LinkedList<TableFieldInfo> fieldInfos = new LinkedList<>();
            fieldInfos.add(mock(TableFieldInfo.class));
            when(fieldRenameNode.getFieldsMapping()).thenReturn(fieldInfos);
            dagNodes.add(fieldRenameNode);

            when(jsProcessorNode.getScript()).thenReturn("script content");
            when(jsProcessorNode.getDeclareScript()).thenReturn("declare content");
            dagNodes.add(jsProcessorNode);

            LinkedList<DatabaseNode> targetNodes = new LinkedList<>();
            DatabaseNode targetNode = mock(DatabaseNode.class);
            when(dag.getTargetNode()).thenReturn(targetNodes);
            when(targetNode.getSyncObjects()).thenReturn(Arrays.asList(mock(SyncObjects.class)));
            targetNodes.add(targetNode);

            int hash = taskDagServiceImpl.calculationDagHash(taskDto);
            assertNotEquals(0, hash, "Hash should not be zero");
        }

        @Test
        public void testCalculationDagHash_RandomReturn() {
            int randomHash = taskDagServiceImpl.calculationDagHash(taskDto);
            assertNotEquals(0, randomHash);
        }
    }
}
