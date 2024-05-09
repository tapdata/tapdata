package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.dynamic.DynamicTableConfig;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableNameUtil;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TableNodeTest {
    TableNode node;
    DynamicTableConfig dynamicTableRule;
    Boolean needDynamicTableName;

    @BeforeEach
    void init() {
        node = mock(TableNode.class);
        dynamicTableRule = mock(DynamicTableConfig.class);
        ReflectionTestUtils.setField(node, "dynamicTableRule", dynamicTableRule);
        ReflectionTestUtils.setField(node, "tableName", "table");
        ReflectionTestUtils.setField(node, "needDynamicTableName", needDynamicTableName);
    }
    @Nested
    class DynamicTableNameTest {
        DynamicTableResult dynamicTable;
        @BeforeEach
        void init() {
            dynamicTable = DynamicTableResult.of().withDynamicName("newName").withOldTableName("table");
            when(node.getSchemaName()).thenReturn("table");
            doCallRealMethod().when(node).dynamicTableName();
            when(node.getOldTableName()).thenCallRealMethod();
            when(node.getTableName()).thenCallRealMethod();
            when(node.getDynamicTableRule()).thenCallRealMethod();
            when(node.getNeedDynamicTableName()).thenCallRealMethod();
        }

        @Test
        void testGetNeedDynamicTableName() {
            Boolean needDynamicTableName = node.getNeedDynamicTableName();
            Assertions.assertNull(needDynamicTableName);
        }

        @Test
        void testGetRule() {
            DynamicTableConfig dynamicTableRule = node.getDynamicTableRule();
            Assertions.assertEquals(dynamicTableRule, dynamicTableRule);
        }

        @Test
        void testNotNeedDynamicTableName() {
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName());
                Assertions.assertEquals("table", node.getTableName());
                Assertions.assertNull(node.getOldTableName());
            }
        }
        @Test
        void testNeedDynamicTableNameButResultIsEmpty() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName());
                Assertions.assertEquals("table", node.getTableName());
                Assertions.assertNull(node.getOldTableName());
            }
        }

        @Test
        void testNeedDynamicTableName() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName());
                Assertions.assertEquals("newName", node.getTableName());
                Assertions.assertEquals("table", node.getOldTableName());
            }
        }
        @Test
        void testDynamicTableConfigIsNull() {
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName());
                Assertions.assertEquals("newName", node.getTableName());
                Assertions.assertEquals("table", node.getOldTableName());
            }
        }
    }

    @Nested
    class GetSchemaNameTest {
        @BeforeEach
        void init() {
            when(node.getSchemaName()).thenCallRealMethod();
        }

        @Test
        void testOldTableNameIsNull() {
            ReflectionTestUtils.setField(node, "oldTableName", null);
            Assertions.assertEquals("table", node.getSchemaName());
        }
        @Test
        void testOldTableNameNotNull() {
            ReflectionTestUtils.setField(node, "oldTableName", "oldTable");
            Assertions.assertEquals("oldTable", node.getSchemaName());
        }
        @Test
        void testDynamicTableConfigIsNull() {
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            ReflectionTestUtils.setField(node, "oldTableName", "oldTable");
            Assertions.assertEquals("oldTable", node.getSchemaName());
        }
        @Test
        void testDynamicTableConfigNotNullButAfterDynamicTableNameNotNull() {
            DynamicTableConfig of = DynamicTableConfig.of();
            of.setAfterDynamicTableName("t");
            ReflectionTestUtils.setField(node, "dynamicTableRule", of);
            ReflectionTestUtils.setField(node, "oldTableName", "oldTable");
            Assertions.assertEquals("table", node.getSchemaName());
        }
        @Test
        void testDynamicTableConfigNotNullButAfterDynamicTableNameEqualsTableName() {
            DynamicTableConfig of = DynamicTableConfig.of();
            of.setAfterDynamicTableName("table");
            ReflectionTestUtils.setField(node, "dynamicTableRule", of);
            ReflectionTestUtils.setField(node, "oldTableName", "oldTable");
            Assertions.assertEquals("oldTable", node.getSchemaName());
        }
    }
}