package com.tapdata.tm.permissions.constants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;

class DataPermissionDataTypeEnumsTest {
    @Test
    void testApiServer() {
        Assertions.assertEquals("ApiServer", DataPermissionDataTypeEnums.ApiServer.getCollection());
        LinkedHashSet<String> actions = DataPermissionDataTypeEnums.ApiServer.allActions();
        Assertions.assertEquals(3, actions.size());
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.View.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Edit.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Delete.name()));
    }

    @Test
    void testApplication() {
        Assertions.assertEquals("Application", DataPermissionDataTypeEnums.Application.getCollection());
        LinkedHashSet<String> actions = DataPermissionDataTypeEnums.Application.allActions();
        Assertions.assertEquals(3, actions.size());
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.View.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Delete.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Edit.name()));
    }

    @Test
    void testModules() {
        Assertions.assertEquals("Modules", DataPermissionDataTypeEnums.Modules.getCollection());
        LinkedHashSet<String> actions = DataPermissionDataTypeEnums.Modules.allActions();
        Assertions.assertEquals(5, actions.size());
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.View.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Edit.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Publish.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Delete.name()));
        Assertions.assertTrue(actions.contains(DataPermissionActionEnums.Revoke.name()));
    }

    @Test
    void testInspect() {
        Assertions.assertEquals("Inspect", DataPermissionDataTypeEnums.INSPECT.getCollection());
        LinkedHashSet<String> actions = DataPermissionDataTypeEnums.INSPECT.allActions();
        Assertions.assertEquals(5, actions.size());
    }

    @Test
    void testParse() {
        Assertions.assertNotNull(DataPermissionDataTypeEnums.parse("Connections"));
        Assertions.assertNotNull(DataPermissionDataTypeEnums.parse("ApiServer"));
        Assertions.assertNotNull(DataPermissionDataTypeEnums.parse("Application"));
        Assertions.assertNotNull(DataPermissionDataTypeEnums.parse("Task"));
        Assertions.assertNotNull(DataPermissionDataTypeEnums.parse("Inspect"));
        Assertions.assertNotNull(DataPermissionDataTypeEnums.parse("Modules"));
    }
}
