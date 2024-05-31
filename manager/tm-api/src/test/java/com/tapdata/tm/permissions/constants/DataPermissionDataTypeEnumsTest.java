package com.tapdata.tm.permissions.constants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;

class DataPermissionDataTypeEnumsTest {
    @Test
    void testInspect() {
        Assertions.assertEquals("Inspect", DataPermissionDataTypeEnums.INSPECT.getCollection());
        LinkedHashSet<String> actions = DataPermissionDataTypeEnums.INSPECT.allActions();
        Assertions.assertEquals(5, actions.size());
    }

    @Test
    void testParse() {
        Assertions.assertNotNull(DataPermissionDataTypeEnums.parse("Inspect"));
    }
}