package com.tapdata.tm.init;

import com.tapdata.tm.utils.OEMReplaceUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class InitLogMapTest {
    @DisplayName("test log not oem start banner")
    @Test
    void test1(){
        assertDoesNotThrow(() -> {InitLogMap.complete(PatchesRunner.class);});
    }
    @DisplayName("test log oem start banner")
    @Test
    void test2(){
        try (MockedStatic<OEMReplaceUtil> oem = org.mockito.Mockito.mockStatic(OEMReplaceUtil.class)) {
            oem.when(()->OEMReplaceUtil.oemType()).thenReturn("data");
            assertDoesNotThrow(() -> {InitLogMap.complete(PatchesRunner.class);});
        }
    }
}
