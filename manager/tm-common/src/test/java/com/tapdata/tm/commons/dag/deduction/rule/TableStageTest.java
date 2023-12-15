package com.tapdata.tm.commons.dag.deduction.rule;

import com.tapdata.tm.commons.schema.Field;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TableStageTest {
    TableStage nodeStage;
    @BeforeEach
    void init() {
        nodeStage = mock(TableStage.class);
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