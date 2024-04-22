package com.tapdata.tm.ds.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.ds.service.impl.PkdSourceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PdkControllerTest {
    private PdkController pdkController;
    private PkdSourceService pkdSourceService;
    @BeforeEach
    void buildMember(){
        pdkController = new PdkController();
        pkdSourceService = mock(PkdSourceService.class);
        ReflectionTestUtils.setField(pdkController,"pkdSourceService",pkdSourceService);
    }
    @Nested
    class checkFileMd5Test{
        @Test
        void testCheckFileMd5(){
            when(pkdSourceService.checkJarMD5("111",14)).thenReturn("123456");
            ResponseMessage<String> actual = pdkController.checkFileMd5("111", 14);
            assertEquals("123456",actual.getData());
        }
        @Test
        void testCheckFileMd5CompatibleOldEngine(){
            when(pkdSourceService.checkJarMD5("111","a.jar")).thenReturn("123456");
            ResponseMessage<String> actual = pdkController.checkFileMd5("111", "a.jar");
            assertEquals("123456",actual.getData());
        }
    }
}
