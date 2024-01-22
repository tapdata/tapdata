package com.tapdata.constant;

import io.tapdata.service.FileCollectorInterface;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FileUtilTest {
    @Test
    public void testHandleFullFilename() {
        assertEquals("filename", FileUtil.handleFullFilename("filename"));
    }

    @Test
    public void testFileLineNumber() throws Exception {
        assertEquals(0L, FileUtil.fileLineNumber("directoryFilePath"));
    }

}
