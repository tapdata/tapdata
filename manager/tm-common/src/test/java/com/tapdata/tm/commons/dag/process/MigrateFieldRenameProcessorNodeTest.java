package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MigrateFieldRenameProcessorNodeTest {
    @Nested
    class ApplyConfigTest {
        @Test
        void testApplyConfigConstructor() {
            MigrateFieldRenameProcessorNode.ApplyConfig config = new MigrateFieldRenameProcessorNode.ApplyConfig(mock(MigrateFieldRenameProcessorNode.class));
            assertNotNull(config.targetFieldExistMaps);
        }
        @Test
        void testApplyConfigConstructorWithTableFieldInfos() {
            MigrateFieldRenameProcessorNode node = mock(MigrateFieldRenameProcessorNode.class);
            LinkedList<TableFieldInfo> tableFieldInfos = new LinkedList<>();
            TableFieldInfo tableFieldInfo = new TableFieldInfo();
            tableFieldInfo.setPreviousTableName("t1");
            LinkedList<FieldInfo> fields = new LinkedList<>();
            FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.setSourceFieldName("A");
            fieldInfo.setTargetFieldName("B");
            FieldInfo fieldInfo1 = new FieldInfo();
            fieldInfo1.setSourceFieldName("B");
            fieldInfo1.setTargetFieldName("C");
            fields.add(fieldInfo);
            fields.add(fieldInfo1);
            tableFieldInfo.setFields(fields);
            tableFieldInfos.add(tableFieldInfo);
            when(node.getFieldsMapping()).thenReturn(tableFieldInfos);
            MigrateFieldRenameProcessorNode.ApplyConfig config = new MigrateFieldRenameProcessorNode.ApplyConfig(node);
            assertTrue(config.targetFieldExistMaps.containsKey("t1"));
            assertTrue(config.targetFieldExistMaps.get("t1").contains("B"));
        }
    }
}
