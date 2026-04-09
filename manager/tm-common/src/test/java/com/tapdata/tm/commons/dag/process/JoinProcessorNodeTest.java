package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.schema.*;
import io.tapdata.entity.schema.TapIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class JoinProcessorNodeTest {
    static class TestableJoinProcessorNode extends JoinProcessorNode {
        void callMergePks(List<Field> fields) {
            super.mergePks(fields);
        }

        int callEachPk(List<String> pks, Map<String, Field> collectField, int position) {
            return super.eachPk(pks, collectField, position);
        }
    }

    @Nested
    class MergeSchema{
        private JoinProcessorNode joinProcessorNode;
        @BeforeEach
        void init(){
            joinProcessorNode = mock(JoinProcessorNode.class);
            ReflectionTestUtils.setField(joinProcessorNode,"embeddedMode",false);
            ReflectionTestUtils.setField(joinProcessorNode,"leftNodeId","leftNodeId");
            ReflectionTestUtils.setField(joinProcessorNode,"rightNodeId","rightNodeId");
            doCallRealMethod().when(joinProcessorNode).addMergeFieldAndPK(anyList(), anyList(), anyList(), anyBoolean());
            when(joinProcessorNode.getUniqueIndices(anyList(), anySet())).thenCallRealMethod();
            when(joinProcessorNode.mergeSchema(null,null,null)).thenCallRealMethod();
        }
        @DisplayName("test left join, left table have delete field")
        @Test
        void test1(){
            List<Node<Schema>> nodeList = new ArrayList<>();
            Node<Schema> leftNode = mock(Node.class);
            when(leftNode.getId()).thenReturn("leftNodeId");
            List<Field> leftField=new ArrayList<>();
            Field leftIdField = new Field();
            leftIdField.setFieldName("id");
            leftIdField.setPrimaryKey(true);
            leftIdField.setPrimaryKeyPosition(1);
            Field sameNameField =new Field();
            sameNameField.setFieldName("sameNameField");
            leftField.add(leftIdField);
            leftField.add(sameNameField);
            Schema leftSchema=new Schema();
            leftSchema.setFields(leftField);
            List<TableIndex> leftSchemaIndex = new ArrayList<>();
            TableIndex tableIndex = new TableIndex();
            tableIndex.setUnique(true);

            TableIndexColumn tableIndexColumn = new TableIndexColumn();
            tableIndexColumn.setColumnName("id");
            List<TableIndexColumn> tableIndexColumns = new ArrayList<>();
            tableIndexColumns.add(tableIndexColumn);

            tableIndex.setColumns(tableIndexColumns);
            leftSchemaIndex.add(tableIndex);
            leftSchema.setIndices(leftSchemaIndex);

            when(leftNode.getOutputSchema()).thenReturn(leftSchema);
            nodeList.add(leftNode);

            Node<Schema> rightNode = mock(Node.class);
            when(rightNode.getId()).thenReturn("rightNodeId");
            List<Field> rightField=new ArrayList<>();
            Field rightIdField = new Field();
            rightIdField.setFieldName("id");
            rightIdField.setPrimaryKey(true);
            rightIdField.setPrimaryKeyPosition(1);
            Field rightSameNameField =new Field();
            rightSameNameField.setFieldName("sameNameField");
            rightSameNameField.setDeleted(true);
            rightField.add(rightIdField);
            rightField.add(rightSameNameField);
            Schema rightSchema=new Schema();
            rightSchema.setFields(rightField);
            when(rightNode.getOutputSchema()).thenReturn(rightSchema);
            nodeList.add(rightNode);

            when(joinProcessorNode.predecessors()).thenReturn(nodeList);

            Schema joinOutPutSchema = joinProcessorNode.mergeSchema(null, null, null);
            assertEquals(3, joinOutPutSchema.getFields().size());
            assertEquals(1, joinOutPutSchema.getIndices().size());
        }
        @DisplayName("test left join, right table have delete field")
        @Test
        void test2(){
            List<Node<Schema>> nodeList = new ArrayList<>();
            Node<Schema> leftNode = mock(Node.class);
            when(leftNode.getId()).thenReturn("leftNodeId");
            List<Field> leftField=new ArrayList<>();
            Field leftIdField = new Field();
            leftIdField.setFieldName("id");
            leftIdField.setPrimaryKey(false);
            leftIdField.setPrimaryKeyPosition(0);
            Field sameNameField =new Field();
            sameNameField.setFieldName("sameNameField");
            sameNameField.setDeleted(true);
            leftField.add(leftIdField);
            leftField.add(sameNameField);
            Schema leftSchema=new Schema();
            leftSchema.setFields(leftField);

            List<TableIndex> leftSchemaIndex = new ArrayList<>();

            TableIndex tableIndex = new TableIndex();
            TableIndexColumn tableIndexColumnId =new TableIndexColumn();
            tableIndexColumnId.setColumnName("id");
            TableIndexColumn tableIndexColumnName =new TableIndexColumn();
            tableIndexColumnName.setColumnName("sameNameField");
            tableIndex.setUnique(true);
            tableIndex.setIndexName("uq_index");
            List<TableIndexColumn> tableIndexColumns=new ArrayList<>();
            tableIndexColumns.add(tableIndexColumnId);
            tableIndexColumns.add(tableIndexColumnName);
            tableIndex.setColumns(tableIndexColumns);
            leftSchemaIndex.add(tableIndex);
            leftSchema.setIndices(leftSchemaIndex);

            when(leftNode.getOutputSchema()).thenReturn(leftSchema);
            nodeList.add(leftNode);

            Node<Schema> rightNode = mock(Node.class);
            when(rightNode.getId()).thenReturn("rightNodeId");
            List<Field> rightField=new ArrayList<>();
            Field rightIdField = new Field();
            rightIdField.setFieldName("id");
            rightIdField.setPrimaryKey(true);
            rightIdField.setPrimaryKeyPosition(1);
            Field rightSameNameField =new Field();
            rightSameNameField.setFieldName("sameNameField");
            rightField.add(rightIdField);
            rightField.add(rightSameNameField);
            Schema rightSchema=new Schema();
            rightSchema.setFields(rightField);
            when(rightNode.getOutputSchema()).thenReturn(rightSchema);
            nodeList.add(rightNode);

            List<TableIndex> rightSchemaIndex = new ArrayList<>();
            TableIndex rightTableIndex = new TableIndex();
            TableIndexColumn rightTableIndexColumnId =new TableIndexColumn();
            rightTableIndexColumnId.setColumnName("id");
            TableIndexColumn rightTableIndexColumnName =new TableIndexColumn();
            rightTableIndexColumnName.setColumnName("sameNameField");
            rightTableIndex.setUnique(true);
            rightTableIndex.setIndexName("right_uq_index");
            List<TableIndexColumn> rightTableIndexColumns=new ArrayList<>();
            rightTableIndexColumns.add(tableIndexColumnId);
            rightTableIndexColumns.add(tableIndexColumnName);
            tableIndex.setColumns(rightTableIndexColumns);
            rightSchemaIndex.add(rightTableIndex);
            rightSchema.setIndices(rightSchemaIndex);

            when(joinProcessorNode.predecessors()).thenReturn(nodeList);

            Schema joinOutPutSchema = joinProcessorNode.mergeSchema(null, null, null);
            assertEquals(3, joinOutPutSchema.getFields().size());
            assertEquals(1, joinOutPutSchema.getIndices().size());
        }
    }

    @Nested
    class MergePksAndEachPk {
        private TestableJoinProcessorNode joinProcessorNode;

        @BeforeEach
        void init() {
            joinProcessorNode = new TestableJoinProcessorNode();
        }

        @Test
        void testEachPk_ShouldSetPkFlagAndPositionAndReturnNextPosition() {
            Field f1 = new Field();
            f1.setFieldName("id");
            Field f2 = new Field();
            f2.setFieldName("rid");
            Map<String, Field> collect = new HashMap<>();
            collect.put("id", f1);
            collect.put("rid", f2);

            int next = joinProcessorNode.callEachPk(List.of("id", "rid"), collect, 1);
            assertEquals(3, next);
            assertTrue(f1.getPrimaryKey());
            assertEquals(1, f1.getPrimaryKeyPosition());
            assertTrue(f2.getPrimaryKey());
            assertEquals(2, f2.getPrimaryKeyPosition());
        }

        @Test
        void testEachPk_EmptyList_ShouldReturnSamePosition() {
            Field f1 = new Field();
            f1.setFieldName("id");
            Map<String, Field> collect = new HashMap<>();
            collect.put("id", f1);
            int next = joinProcessorNode.callEachPk(List.of(), collect, 5);
            assertEquals(5, next);
            assertFalse(Boolean.TRUE.equals(f1.getPrimaryKey()));
        }

        @Test
        void testMergePks_ShouldMergeLeftThenRightPositions() {
            joinProcessorNode.setLeftPrimaryKeys(List.of("id"));
            joinProcessorNode.setRightPrimaryKeys(List.of("rid"));

            Field id = new Field();
            id.setFieldName("id");
            Field rid = new Field();
            rid.setFieldName("rid");
            Field normal = new Field();
            normal.setFieldName("name");

            List<Field> fields = new ArrayList<>();
            fields.add(id);
            fields.add(rid);
            fields.add(normal);

            joinProcessorNode.callMergePks(fields);
            assertTrue(id.getPrimaryKey());
            assertEquals(1, id.getPrimaryKeyPosition());
            assertTrue(rid.getPrimaryKey());
            assertEquals(2, rid.getPrimaryKeyPosition());
            assertFalse(Boolean.TRUE.equals(normal.getPrimaryKey()));
        }

        @Test
        void testMergePks_DuplicateFieldName_ShouldUseLastOne() {
            joinProcessorNode.setLeftPrimaryKeys(List.of("id"));
            joinProcessorNode.setRightPrimaryKeys(List.of());

            Field first = new Field();
            first.setFieldName("id");
            Field last = new Field();
            last.setFieldName("id");

            List<Field> fields = new ArrayList<>();
            fields.add(first);
            fields.add(last);

            joinProcessorNode.callMergePks(fields);
            assertNotNull(last.getPrimaryKey());
            assertTrue(last.getPrimaryKey());
            assertEquals(1, last.getPrimaryKeyPosition());
            assertFalse(Boolean.TRUE.equals(first.getPrimaryKey()));
        }

        @Test
        void testMergePks_WhenPkNotFound_ShouldThrow() {
            joinProcessorNode.setLeftPrimaryKeys(List.of("missing"));
            joinProcessorNode.setRightPrimaryKeys(List.of());
            List<Field> fields = new ArrayList<>();
            Field f = new Field();
            f.setFieldName("id");
            fields.add(f);
            assertThrows(NullPointerException.class, () -> joinProcessorNode.callMergePks(fields));
        }
    }
}
