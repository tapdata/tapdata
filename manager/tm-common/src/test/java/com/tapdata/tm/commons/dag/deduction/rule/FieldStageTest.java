package com.tapdata.tm.commons.dag.deduction.rule;


import com.tapdata.tm.commons.dag.deduction.rule.field.InFieldStage;
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

class FieldStageTest {
    FieldStage fieldStage;
    @BeforeEach
    void init() {
        fieldStage = mock(FieldStage.class);
    }
    @Nested
    class ChangeTest {
        @Test
        void normal() {
            doCallRealMethod().when(fieldStage).change(anyList(), any(MetadataInstancesDto.class));
            List<FieldChangeRule> changeRules = mock(List.class);
            MetadataInstancesDto metadataInstancesDto = mock(MetadataInstancesDto.class);
            when(fieldStage.groupField(metadataInstancesDto)).thenReturn(null);
            try (MockedStatic<InFieldStage> stageMockedStatic = mockStatic(InFieldStage.class)) {
                stageMockedStatic.when(()->InFieldStage.startStage(changeRules, null)).thenAnswer(w->null);
                fieldStage.change(changeRules, metadataInstancesDto);
                stageMockedStatic.verify(()->InFieldStage.startStage(changeRules, null), times(1));
            } finally {
                verify(fieldStage, times(1)).groupField(metadataInstancesDto);
            }
        }
    }
    @Nested
    class GroupByTest {
        @Test
        void testGroupBy() {
            when(fieldStage.groupBy()).thenCallRealMethod();
            Function<Field, String> function = fieldStage.groupBy();
            Assertions.assertNotNull(function);
        }
    }
}