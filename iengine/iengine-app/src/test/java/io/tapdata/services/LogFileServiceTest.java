package io.tapdata.services;

import io.tapdata.observable.logging.appender.FileAppender;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mockConstruction;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/26 15:52
 */
public class LogFileServiceTest {

    @Test
    void testDescribeLogFiles() {

        LogFileService logFileService = new LogFileService();

        try (MockedStatic<FileUtils> fileUtils = mockStatic(FileUtils.class);) {

            Collection<File> files = new ArrayList<>();
            files.add(new File("test.log"));
            files.add(new File("taskId.log"));

            fileUtils.when(() -> FileUtils.listFiles(any(), eq(null), eq(true)) ).thenReturn(files);

            List<Map<String, Object>> result = logFileService.describeLogFiles(null);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(2, result.size());

            result = logFileService.describeLogFiles("taskId");
            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.size());

            files.add(new File("taskId_debug.log"));
            result = logFileService.describeLogFiles("taskId");
            Assertions.assertNotNull(result);
            Assertions.assertEquals(2, result.size());
        }
    }

    @Test
    void testDeleteFile() {
        LogFileService logFileService = new LogFileService();

        String result = logFileService.deleteLogFile("../test.log");
        Assertions.assertNotNull(result);
        Assertions.assertEquals("Only delete log file in logs directory", result);

        result = logFileService.deleteLogFile("test.log");
        Assertions.assertNotNull(result);
        Assertions.assertEquals("Not exists", result);

        try (MockedStatic<FileUtils> fileUtils = mockStatic(FileUtils.class)) {

            File logFile = mock(File.class);
            File logDirectory = mock(File.class);
            fileUtils.when(() -> FileUtils.getFile(eq(FileAppender.LOG_PATH))).thenReturn(logDirectory);
            fileUtils.when(() -> FileUtils.getFile(eq(FileAppender.LOG_PATH), eq("taskId.log"))).thenReturn(logFile);

            when(logDirectory.getAbsolutePath()).thenReturn("/test");
            when(logFile.getParentFile()).thenReturn(logDirectory);

            when(logFile.exists()).thenReturn(true);
            when(logFile.isFile()).thenReturn(false);
            result = logFileService.deleteLogFile("taskId.log");
            Assertions.assertNotNull(result);
            Assertions.assertEquals("Not is file", result);

            when(logFile.exists()).thenReturn(true);
            when(logFile.isFile()).thenReturn(true);
            when(logFile.delete()).thenReturn(true);
            result = logFileService.deleteLogFile("taskId.log");
            Assertions.assertNotNull(result);
            Assertions.assertEquals("ok", result);

            when(logFile.exists()).thenReturn(true);
            when(logFile.isFile()).thenReturn(true);
            when(logFile.delete()).thenReturn(false);
            result = logFileService.deleteLogFile("taskId.log");
            Assertions.assertNotNull(result);
            Assertions.assertEquals("failed", result);
        }
    }

}
