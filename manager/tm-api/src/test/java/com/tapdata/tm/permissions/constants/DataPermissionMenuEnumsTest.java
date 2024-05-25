package com.tapdata.tm.permissions.constants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DataPermissionMenuEnumsTest {
    @Test
    void testInspect() {
        Assertions.assertEquals("", DataPermissionMenuEnums.InspectTack.getAllDataPermissionName());
        Assertions.assertEquals(DataPermissionDataTypeEnums.Inspect, DataPermissionMenuEnums.InspectTack.getDataType());
    }
}