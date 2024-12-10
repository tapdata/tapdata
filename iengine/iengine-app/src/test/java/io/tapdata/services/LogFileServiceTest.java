package io.tapdata.services;

import io.tapdata.modules.api.net.data.FileMeta;
import io.tapdata.observable.logging.appender.FileAppender;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
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

    LogFileService logFileService;

    @BeforeEach
    void setup() {
        logFileService = new LogFileService();
    }

    @Test
    void testDescribeLogFiles() {

        try (MockedStatic<FileUtils> fileUtils = mockStatic(FileUtils.class);
             MockedStatic<Files> mockFiles = mockStatic(Files.class);) {

            BasicFileAttributes attributes = mock(BasicFileAttributes.class);
            mockFiles.when(() -> Files.readAttributes(any(), eq(BasicFileAttributes.class)))
                    .thenReturn(attributes);
            when(attributes.creationTime()).thenReturn(FileTime.fromMillis(System.currentTimeMillis()));
            when(attributes.lastAccessTime()).thenReturn(FileTime.fromMillis(System.currentTimeMillis()));

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
            Assertions.assertEquals(5, result.get(0).size());
        }
    }

    @Test
    void testDeleteFile() {

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

    @Nested
    class testDownloadFile {
        @Test
        void testFileNotExists() {
            FileMeta fileMeta = logFileService.downloadFile("a.log");
            Assertions.assertNotNull(fileMeta);
            Assertions.assertEquals("FileNotFound", fileMeta.getCode());
        }

        @Test
        void testUnCompressionFile() throws IOException {
            File file = new File(FileAppender.LOG_PATH, "test.log");
            final Path dir = Files.createDirectories(file.getParentFile().toPath());
            final Path filePath = Files.createFile(file.toPath());

            FileMeta fileMeta = logFileService.downloadFile(file.getName());

            Assertions.assertNotNull(fileMeta);
            Assertions.assertEquals("ok", fileMeta.getCode());
            Assertions.assertEquals("test.log.zip", fileMeta.getFilename());
            Assertions.assertNotNull(fileMeta.getFileInputStream());

            Files.delete(filePath);
            Files.delete(new File(FileAppender.LOG_PATH, fileMeta.getFilename()).toPath());
            Files.deleteIfExists(dir);
        }

        @Test
        void testCompressionFile() throws IOException {
            File file = new File(FileAppender.LOG_PATH, "test.log.zip");
            final Path dir = Files.createDirectories(file.getParentFile().toPath());
            final Path filePath = Files.createFile(file.toPath());

            FileMeta fileMeta = logFileService.downloadFile(file.getName());

            Assertions.assertNotNull(fileMeta);
            Assertions.assertEquals("ok", fileMeta.getCode());
            Assertions.assertEquals("test.log.zip", fileMeta.getFilename());
            Assertions.assertNotNull(fileMeta.getFileInputStream());
            Assertions.assertEquals(0, fileMeta.getFileSize());

            Files.delete(filePath);
            Files.deleteIfExists(dir);
        }
    }

}
