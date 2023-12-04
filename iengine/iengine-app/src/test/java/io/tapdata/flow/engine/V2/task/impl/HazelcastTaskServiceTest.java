package io.tapdata.flow.engine.V2.task.impl;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HazelcastTaskServiceTest {

    @Nested
    class SetTaskDtoIsomorphismTest {
        HazelcastTaskService service;
        List<Node> nodes;
        TaskDto taskDto;
        Node source;
        Node target;
        Node other;
        @BeforeEach
        void init() {
            service = mock(HazelcastTaskService.class);

            nodes = new ArrayList<>();
            taskDto = mock(TaskDto.class);
            doCallRealMethod().when(taskDto).setIsomorphism(anyBoolean());
            when(taskDto.getIsomorphism()).thenCallRealMethod();

            source = mock(DataParentNode.class);
            target = mock(DataParentNode.class);
            when(((DataParentNode)source).getDatabaseType()).thenReturn("Dummy");
            when(((DataParentNode)target).getDatabaseType()).thenReturn("Dummy");
            other = mock(ProcessorNode.class);

            doCallRealMethod().when(service).setTaskDtoIsomorphism(anyList(), any(TaskDto.class));
            doCallRealMethod().when(service).setTaskDtoIsomorphism(nodes, null);
            doCallRealMethod().when(service).setTaskDtoIsomorphism(null, taskDto);
        }

        @Test
        void testSetTaskDtoIsomorphismNullTaskDto() {
            nodes.add(source);
            nodes.add(target);
            nodes.add(other);
            Assertions.assertThrows(IllegalArgumentException.class, () -> service.setTaskDtoIsomorphism(nodes, null));
        }

        @Test
        void testSetTaskDtoIsomorphismNullNodes() {
            service.setTaskDtoIsomorphism(null, taskDto);
            Boolean isomorphism = taskDto.getIsomorphism();
            Assertions.assertFalse(isomorphism);
        }

        @Test
        void testSetTaskDtoIsomorphismEmptyNodes() {
            service.setTaskDtoIsomorphism(nodes, taskDto);
            Boolean isomorphism = taskDto.getIsomorphism();
            Assertions.assertFalse(isomorphism);
        }

        @Test
        void testSetTaskDtoIsomorphismIsIsomorphismButContainsOtherNodes() {
            nodes.add(source);
            nodes.add(target);
            nodes.add(other);
            service.setTaskDtoIsomorphism(nodes, taskDto);
            Boolean isomorphism = taskDto.getIsomorphism();
            Assertions.assertFalse(isomorphism);
        }

        @Test
        void testSetTaskDtoIsomorphismTaskIsIsomorphismAndNotContainsOtherNodes() {
            nodes.add(source);
            nodes.add(target);
            service.setTaskDtoIsomorphism(nodes, taskDto);
            Boolean isomorphism = taskDto.getIsomorphism();
            Assertions.assertTrue(isomorphism);
        }

        @Test
        void testSetTaskDtoIsomorphismTaskNotIsomorphismAndNotContainsOtherNodes() {
            when(((DataParentNode)target).getDatabaseType()).thenReturn("Eummy");
            nodes.add(source);
            nodes.add(target);
            service.setTaskDtoIsomorphism(nodes, taskDto);
            Boolean isomorphism = taskDto.getIsomorphism();
            Assertions.assertFalse(isomorphism);
        }

        @Test
        void testSetTaskDtoIsomorphismTaskNotIsomorphismButContainsOtherNodes() {
            when(((DataParentNode)target).getDatabaseType()).thenReturn("Eummy");
            nodes.add(source);
            nodes.add(target);
            nodes.add(other);
            service.setTaskDtoIsomorphism(nodes, taskDto);
            Boolean isomorphism = taskDto.getIsomorphism();
            Assertions.assertFalse(isomorphism);
        }
    }
}