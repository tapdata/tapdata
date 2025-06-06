package io.tapdata.services;

import io.tapdata.modules.api.net.data.FileMeta;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


public class ExportEventSqlServiceTest {

    private ExportEventSqlService exportEventSqlService;
    private static final String TEST_INSPECT_ID = "test-inspect-id";
    private static final String TEST_INSPECT_RESULT_ID = "test-inspect-result-id";
    private static final String EXPORT_SQL = "exportSql";

    @BeforeEach
    void setUp() {
        exportEventSqlService = new ExportEventSqlService();
    }

    @Nested
    @DisplayName("downloadEventSql Tests")
    class DownloadEventSqlTests {

        @Test
        @DisplayName("Should return file not exists when file does not exist")
        void testDownloadEventSql_FileNotExists() {
            try (MockedStatic<Files> mockFiles = mockStatic(Files.class)) {
                Path mockPath = mock(Path.class);
                mockFiles.when(() -> Files.exists(any(Path.class))).thenReturn(false);

                FileMeta result = exportEventSqlService.downloadEventSql(TEST_INSPECT_ID, TEST_INSPECT_RESULT_ID);

                assertNotNull(result);
                assertEquals(TEST_INSPECT_RESULT_ID + ".sql", result.getFilename());
                assertEquals("File not exists.", result.getCode());
                assertFalse(result.isTransferFile());
                assertNull(result.getFileSize());
                assertNull(result.getFileInputStream());
            }
        }

        @Test
        @DisplayName("Should return file meta when file exists and is readable")
        void testDownloadEventSql_FileExists() throws IOException {
            try (MockedStatic<Files> mockFiles = mockStatic(Files.class);
                 MockedStatic<Paths> mockPaths = mockStatic(Paths.class)) {

                Path mockPath = mock(Path.class);
                InputStream mockInputStream = mock(InputStream.class);
                long fileSize = 1024L;
                
                mockPaths.when(() -> Paths.get(anyString())).thenReturn(mockPath);
                mockFiles.when(() -> Files.exists(mockPath)).thenReturn(true);
                mockFiles.when(() -> Files.size(mockPath)).thenReturn(fileSize);
                mockFiles.when(() -> Files.newInputStream(mockPath, StandardOpenOption.READ)).thenReturn(mockInputStream);

                FileMeta result = exportEventSqlService.downloadEventSql(TEST_INSPECT_ID, TEST_INSPECT_RESULT_ID);

                assertNotNull(result);
                assertEquals(TEST_INSPECT_RESULT_ID + ".sql", result.getFilename());
                assertEquals("ok", result.getCode());
                assertTrue(result.isTransferFile());
                assertEquals(fileSize, result.getFileSize());
                assertEquals(mockInputStream, result.getFileInputStream());
            }
        }

        @Test
        @DisplayName("Should return read file error when exception occurs")
        void testDownloadEventSql_ExceptionOccurs() throws IOException {
            try (MockedStatic<Files> mockFiles = mockStatic(Files.class);
                 MockedStatic<Paths> mockPaths = mockStatic(Paths.class)) {

                Path mockPath = mock(Path.class);
                long fileSize = 1024L;

                mockPaths.when(() -> Paths.get(anyString())).thenReturn(mockPath);
                mockFiles.when(() -> Files.exists(mockPath)).thenReturn(true);
                mockFiles.when(() -> Files.size(mockPath)).thenReturn(fileSize);
                mockFiles.when(() -> Files.newInputStream(mockPath, StandardOpenOption.READ)).thenThrow(new IOException("Read file error"));

                FileMeta result = exportEventSqlService.downloadEventSql(TEST_INSPECT_ID, TEST_INSPECT_RESULT_ID);


                assertNotNull(result);
                assertEquals(TEST_INSPECT_RESULT_ID + ".sql", result.getFilename());
                assertEquals("ReadFileError", result.getCode());
                assertFalse(result.isTransferFile());
                assertNull(result.getFileSize());
                assertNull(result.getFileInputStream());
            }
        }

        @Test
        @DisplayName("Should handle null inspect ID")
        void testDownloadEventSql_NullInspectId() {

            FileMeta result = exportEventSqlService.downloadEventSql(null, TEST_INSPECT_RESULT_ID);

            assertNotNull(result);
            assertEquals(TEST_INSPECT_RESULT_ID + ".sql", result.getFilename());
            assertEquals("File not exists.", result.getCode());
            assertFalse(result.isTransferFile());
        }

        @Test
        @DisplayName("Should handle null inspect result ID")
        void testDownloadEventSql_NullInspectResultId() {
            FileMeta result = exportEventSqlService.downloadEventSql(TEST_INSPECT_ID, null);

            assertNotNull(result);
            assertEquals("null.sql", result.getFilename());
            assertEquals("File not exists.", result.getCode());
            assertFalse(result.isTransferFile());
        }

        @Test
        @DisplayName("Should handle empty strings")
        void testDownloadEventSql_EmptyStrings() {
            FileMeta result = exportEventSqlService.downloadEventSql("", "");

            assertNotNull(result);
            assertEquals(".sql", result.getFilename());
            assertEquals("File not exists.", result.getCode());
            assertFalse(result.isTransferFile());
        }
    }

    @Nested
    @DisplayName("deleteEventSql Tests")
    class DeleteEventSqlTests {

        @Test
        @DisplayName("Should delete specific file when inspectResultId is provided")
        void testDeleteEventSql_WithInspectResultId() throws IOException {
            try (MockedStatic<Files> mockFiles = mockStatic(Files.class);
                 MockedStatic<Paths> mockPaths = mockStatic(Paths.class);
                 MockedStatic<StringUtils> mockStringUtils = mockStatic(StringUtils.class)) {

                Path mockPath = mock(Path.class);
                mockPaths.when(() -> Paths.get(anyString())).thenReturn(mockPath);
                mockStringUtils.when(() -> StringUtils.isNotBlank(TEST_INSPECT_RESULT_ID)).thenReturn(true);
                mockFiles.when(() -> Files.deleteIfExists(mockPath)).thenReturn(true);

                assertDoesNotThrow(() -> exportEventSqlService.deleteEventSql(TEST_INSPECT_ID, TEST_INSPECT_RESULT_ID));

                mockFiles.verify(() -> Files.deleteIfExists(mockPath), times(1));
                String expectedPath = EXPORT_SQL + File.separator + TEST_INSPECT_ID + File.separator + TEST_INSPECT_RESULT_ID + ".sql";
                mockPaths.verify(() -> Paths.get(expectedPath), times(1));
            }
        }

        @Test
        @DisplayName("Should delete all files in directory when inspectResultId is null")
        void testDeleteEventSql_WithoutInspectResultId() throws IOException {
            try (MockedStatic<Files> mockFiles = mockStatic(Files.class);
                 MockedStatic<Paths> mockPaths = mockStatic(Paths.class);
                 MockedStatic<StringUtils> mockStringUtils = mockStatic(StringUtils.class)) {

                Path mockDirPath = mock(Path.class);
                Path mockFilePath1 = mock(Path.class);
                Path mockFilePath2 = mock(Path.class);
                Stream<Path> mockStream = Stream.of(mockFilePath1, mockFilePath2);
                
                mockPaths.when(() -> Paths.get(anyString())).thenReturn(mockDirPath);
                mockStringUtils.when(() -> StringUtils.isNotBlank(null)).thenReturn(false);
                mockFiles.when(() -> Files.list(mockDirPath)).thenReturn(mockStream);
                mockFiles.when(() -> Files.isRegularFile(any(Path.class))).thenReturn(true);
                mockFiles.when(() -> Files.deleteIfExists(any(Path.class))).thenReturn(true);

                assertDoesNotThrow(() -> exportEventSqlService.deleteEventSql(TEST_INSPECT_ID, null));

                mockFiles.verify(() -> Files.list(mockDirPath), times(1));
                mockFiles.verify(() -> Files.deleteIfExists(mockFilePath1), times(1));
                mockFiles.verify(() -> Files.deleteIfExists(mockFilePath2), times(1));
                mockFiles.verify(() -> Files.deleteIfExists(mockDirPath), times(1));
            }
        }
    }
}
