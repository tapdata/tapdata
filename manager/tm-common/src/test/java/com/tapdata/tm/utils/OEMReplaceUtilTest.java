package com.tapdata.tm.utils;

import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

public class OEMReplaceUtilTest {
    @Nested
    class getOEMConfigInputStreamTest {
        @Test
        @SneakyThrows
        @DisplayName("test getOEMConfigInputStream method when configPath is null")
        void test1(){
            String fileName = "test";
            try (MockedStatic<OEMReplaceUtil> mb = Mockito
                    .mockStatic(OEMReplaceUtil.class)) {
                mb.when(()->OEMReplaceUtil.getOEMConfigPath(fileName)).thenReturn(null);
                InputStream actual = OEMReplaceUtil.getOEMConfigInputStream(fileName);
                assertNull(actual);
            }
        }
    }
    @Nested
    class getOEMConfigPathTest{
        @Test
        @DisplayName("test getOEMConfigPath method when oemType is null")
        void test1(){
            String fileName = "test";
            String actual = OEMReplaceUtil.getOEMConfigPath(fileName);
            assertNull(actual);
        }
    }
}
