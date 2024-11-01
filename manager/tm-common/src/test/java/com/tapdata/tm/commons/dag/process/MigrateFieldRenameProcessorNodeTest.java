package com.tapdata.tm.commons.dag.process;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class MigrateFieldRenameProcessorNodeTest {
    @Test
    void testApplyConfigConstructor() {
        MigrateFieldRenameProcessorNode.ApplyConfig config = new MigrateFieldRenameProcessorNode.ApplyConfig(mock(MigrateFieldRenameProcessorNode.class));
        assertNotNull(config.getFieldInfoTempMaps());
    }
}
