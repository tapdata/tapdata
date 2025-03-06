package com.tapdata.tm.commons.dag.nodes;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.DAGDataServiceImpl;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableConfig;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableNameUtil;
import com.tapdata.tm.commons.dag.dynamic.DynamicTableResult;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

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
    Schema schema;

    @BeforeEach
    void init() {
        schema = new Schema();
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
            when(node.getSchemaName(schema)).thenReturn("table");
            doCallRealMethod().when(node).dynamicTableName(schema);
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
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(schema));
                Assertions.assertEquals("table", node.getTableName());
            }
        }
        @Test
        void testNotNeedDynamicTableNameSchemaIsNull() {
            doCallRealMethod().when(node).dynamicTableName(null);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(null));
                Assertions.assertNull(node.getTableName());
            }
        }
        @Test
        void testNeedDynamicTableNameButResultIsEmpty() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(schema));
                Assertions.assertEquals("table", node.getTableName());
            }
        }

        @Test
        void testNeedDynamicTableName() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(schema));
                Assertions.assertEquals("newName", node.getTableName());
            }
        }

        @Test
        void testNeedDynamicTableNameSchemaIsNull() {
            doCallRealMethod().when(node).dynamicTableName(null);
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(null));
                Assertions.assertEquals("newName", node.getTableName());
            }
        }
        @Test
        void testDynamicTableConfigIsNull() {
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(schema));
                Assertions.assertEquals("newName", node.getTableName());
            }
        }
        @Test
        void testTableNameEqualsBeforeNameInSchema() {
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            schema.setAfterDynamicTableName("table");
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(schema));
                Assertions.assertEquals("newName", node.getTableName());
            }
        }
        @Test
        void testTableNameIsNull() {
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            ReflectionTestUtils.setField(node, "tableName", null);
            schema.setAfterDynamicTableName("table");
            try(MockedStatic<DynamicTableNameUtil> dtnu = mockStatic(DynamicTableNameUtil.class)) {
                dtnu.when(() -> DynamicTableNameUtil.getDynamicTable(anyString(), any(DynamicTableConfig.class))).thenReturn(dynamicTable);
                Assertions.assertDoesNotThrow(() -> node.dynamicTableName(schema));
                Assertions.assertEquals("newName", node.getTableName());
            }
        }
    }

    @Nested
    class GetSchemaNameTest {
        @BeforeEach
        void init() {
            when(node.getSchemaName(schema)).thenCallRealMethod();
            when(node.getSchemaName(null)).thenCallRealMethod();
        }

        @Test
        void testOldTableNameIsNull() {
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testOldTableNameNotNull() {
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testDynamicTableConfigIsNull() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testDynamicTableConfigNotNullButAfterDynamicTableNameNotNull() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            DynamicTableConfig of = DynamicTableConfig.of();
            ReflectionTestUtils.setField(node, "dynamicTableRule", of);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testDynamicTableConfigNotNullButAfterDynamicTableNameIsNull() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            DynamicTableConfig of = DynamicTableConfig.of();
            ReflectionTestUtils.setField(node, "dynamicTableRule", of);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testDynamicTableConfigNotNullButAfterDynamicTableNameEqualsTableName() {
            ReflectionTestUtils.setField(node, "needDynamicTableName", true);
            DynamicTableConfig of = DynamicTableConfig.of();
            ReflectionTestUtils.setField(node, "dynamicTableRule", of);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testDynamicTableNameIsNull() {
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testBeforeDynamicTableNameNotNullInSchema() {
            schema.setBeforeDynamicTableName("table");
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testAfterDynamicTableNameNotNullInSchema() {
            schema.setAfterDynamicTableName("table");
            DynamicTableConfig of = DynamicTableConfig.of();
            ReflectionTestUtils.setField(node, "dynamicTableRule", of);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testAfterDynamicTableNameNotEqualsTableName() {
            schema.setAfterDynamicTableName("newTable");
            DynamicTableConfig of = DynamicTableConfig.of();
            ReflectionTestUtils.setField(node, "dynamicTableRule", of);
            Assertions.assertEquals("table", node.getSchemaName(schema));
        }
        @Test
        void testSchemaIsNull() {
            ReflectionTestUtils.setField(node, "dynamicTableRule", null);
            Assertions.assertEquals("table", node.getSchemaName(null));
        }
    }

    @Nested
    class UpdateSchemaAfterDynamicTableNameTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(node).updateSchemaAfterDynamicTableName(schema, "table", "newTable");
            node.updateSchemaAfterDynamicTableName(schema, "table", "newTable");
            Assertions.assertEquals("table", schema.getBeforeDynamicTableName());
            Assertions.assertEquals("newTable", schema.getAfterDynamicTableName());
        }
        @Test
        void testNull() {
            String before = schema.getBeforeDynamicTableName();
            String after = schema.getAfterDynamicTableName();
            doCallRealMethod().when(node).updateSchemaAfterDynamicTableName(null, "table", "newTable");
            node.updateSchemaAfterDynamicTableName(null, "table", "newTable");
            Assertions.assertEquals(before, schema.getBeforeDynamicTableName());
            Assertions.assertEquals(after, schema.getAfterDynamicTableName());
        }
    }
    @Nested
    class mergeSchemaTest{
        TableNode tableNode;
        @BeforeEach
        void init() {
            tableNode = mock(TableNode.class);
            ReflectionTestUtils.setField(tableNode, "tableName", "tableName");
            DAGDataServiceImpl dagDataService = mock(DAGDataServiceImpl.class);
            ReflectionTestUtils.setField(tableNode, "connectionId", "connectionId");
            ReflectionTestUtils.setField(tableNode, "service", dagDataService);
            DataSourceConnectionDto dataSourceConnectionDto = new DataSourceConnectionDto();
            dataSourceConnectionDto.setDatabase_type("MongoDB");
            when(dagDataService.getDataSource("connectionId")).thenReturn(dataSourceConnectionDto);
            doCallRealMethod().when(tableNode).mergeSchema(any(), any(), any());
        }

        @Test
        void test_mongoSchema_exist_id(){
            DAG dag = mock(DAG.class);
            when(dag.getSyncType()).thenReturn("logCollector");
            DynamicTableResult dynamicTableResult = DynamicTableResult.of()
                    .withOldTableName("test1")
                    .withDynamicName("test2");
            when(tableNode.dynamicTableName(any())).thenReturn(dynamicTableResult);
            when(tableNode.getDag()).thenReturn(dag);
            List<Schema> schemas = new ArrayList<>();
            Schema schema = new Schema();
            schema.setName("test1");
            schema.setOriginalName("test1");
            schema.setAncestorsName("test1");
            List<Field> fields = new ArrayList<>();
            Field field = new Field();
            field.setFieldName("_id");
            field.setPrimaryKey(true);
            fields.add(field);
            schema.setFields(fields);
            schemas.add(schema);
            when(tableNode.transformFields(any(), any(),any())).thenReturn(fields);
            Schema result = tableNode.mergeSchema(schemas,schema, new DAG.Options());
            Assertions.assertEquals(1,result.getFields().size());
        }

        @Test
        void test_mongoSchema_no_exist_id(){
            DAG dag = mock(DAG.class);
            when(dag.getSyncType()).thenReturn("logCollector");
            DynamicTableResult dynamicTableResult = DynamicTableResult.of()
                    .withOldTableName("test1")
                    .withDynamicName("test2");
            when(tableNode.dynamicTableName(any())).thenReturn(dynamicTableResult);
            when(tableNode.getDag()).thenReturn(dag);
            List<Schema> schemas = new ArrayList<>();
            Schema schema = new Schema();
            schema.setName("test1");
            schema.setOriginalName("test1");
            schema.setAncestorsName("test1");
            List<Field> fields = new ArrayList<>();
            Field field = new Field();
            field.setFieldName("test");
            field.setPrimaryKey(false);
            fields.add(field);
            schema.setFields(fields);
            schemas.add(schema);
            when(tableNode.transformFields(any(), any(),any())).thenReturn(fields);
            Schema result = tableNode.mergeSchema(schemas,schema, new DAG.Options());
            Assertions.assertEquals(2,result.getFields().size());
        }
    }
}