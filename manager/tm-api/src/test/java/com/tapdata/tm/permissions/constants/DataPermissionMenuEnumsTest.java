package com.tapdata.tm.permissions.constants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DataPermissionMenuEnumsTest {
    @Test
    void testInspect() {
        Assertions.assertEquals("", DataPermissionMenuEnums.INSPECT_TACK.getAllDataPermissionName());
        Assertions.assertEquals(DataPermissionDataTypeEnums.INSPECT, DataPermissionMenuEnums.INSPECT_TACK.getDataType());
    }
}