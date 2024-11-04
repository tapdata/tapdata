package com.tapdata.tm.commons.dag.process;

import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MigrateFieldRenameProcessorNodeTest {
    @Nested
    class ApplyConfigTest {
        @Test
        void testApplyConfigConstructor() {
            MigrateFieldRenameProcessorNode.ApplyConfig config = new MigrateFieldRenameProcessorNode.ApplyConfig(mock(MigrateFieldRenameProcessorNode.class));
            assertNotNull(config.getFieldInfoTempMaps());
        }
        @Test
        void testApplyConfigConstructorWithTableFieldInfos() {
            MigrateFieldRenameProcessorNode node = mock(MigrateFieldRenameProcessorNode.class);
            LinkedList<TableFieldInfo> tableFieldInfos = new LinkedList<>();
            TableFieldInfo tableFieldInfo = new TableFieldInfo();
            tableFieldInfo.setPreviousTableName("t1");
            tableFieldInfos.add(tableFieldInfo);
            when(node.getFieldsMapping()).thenReturn(tableFieldInfos);
            MigrateFieldRenameProcessorNode.ApplyConfig config = new MigrateFieldRenameProcessorNode.ApplyConfig(node);
            assertNotNull(config.getFieldInfoTempMaps());
            assertNotNull(config.getFieldInfoTempMaps().get("t1"));
        }
    }
}
