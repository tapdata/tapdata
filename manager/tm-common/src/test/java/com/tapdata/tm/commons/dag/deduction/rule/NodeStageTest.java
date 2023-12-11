package com.tapdata.tm.commons.dag.deduction.rule;


import com.tapdata.tm.commons.dag.deduction.rule.node.InNodeStage;
import com.tapdata.tm.commons.dag.vo.FieldChangeRule;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NodeStageTest {
    NodeStage nodeStage;
    @BeforeEach
    void init() {
        nodeStage = mock(NodeStage.class);
    }
    @Nested
    class ChangeTest {
        @Test
        void normal() {
            doCallRealMethod().when(nodeStage).change(anyList(), any(MetadataInstancesDto.class));
            List<FieldChangeRule> changeRules = mock(List.class);
            MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
            when(nodeStage.groupField(metadataInstancesDto)).thenReturn(null);
            try (MockedStatic<InNodeStage> stageMockedStatic = mockStatic(InNodeStage.class)) {
                stageMockedStatic.when(()->InNodeStage.startStage(changeRules, null)).thenAnswer(w->null);
                nodeStage.change(changeRules, metadataInstancesDto);
                stageMockedStatic.verify(()->InNodeStage.startStage(changeRules, null), times(1));
            } finally {
                verify(nodeStage, times(1)).groupField(metadataInstancesDto);
            }
        }
    }
    @Nested
    class GroupByTest {
        @Test
        void testGroupBy() {
            when(nodeStage.groupBy()).thenCallRealMethod();
            Function<Field, String> function = nodeStage.groupBy();
            Assertions.assertNotNull(function);
        }
    }
}