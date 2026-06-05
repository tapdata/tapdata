package com.tapdata.tm.utils;

import lombok.SneakyThrows;
import org.apache.commons.fileupload.FileItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PdkSourceUtilsTest {
    @Nested
    @DisplayName("getFileMD5 method test")
    class GetFileMD5Test{
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test when parameter class is MultipartFile")
        void testGetFileMD5WithMultipartFile(){
            MockMultipartFile mp = new MockMultipartFile(
                    "a.jar",
                    "a.jar",
                    "application/octet-stream",
                    "content".getBytes(StandardCharsets.UTF_8)
            );
            String md5 = PdkSourceUtils.getFileMD5(mp);
            assertNotEquals(null,md5);
            assertEquals(32, md5.length());
        }

        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 should use existing file when file exists")
        void testGetFileMD5UsesMultipartContentWhenFileExists(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            try {
                FileItem fileItem = mock(FileItem.class);
                when(fileItem.getFieldName()).thenReturn("a.jar");
                MockMultipartFile mp = new MockMultipartFile("a.jar","a.jar","",fileItem.getInputStream());
                String md5 = PdkSourceUtils.getFileMD5(mp);
                assertNotNull(md5);
                assertEquals(md5Hex(new byte[0]), md5);
            } finally {
                existFile.delete();
            }
        }
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test when parameter class is File")
        void testGetFileMD5WithFile(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            try {
                String md5 = PdkSourceUtils.getFileMD5(existFile);
                assertNotNull(md5);
                assertEquals(md5Hex(new byte[0]), md5);
            } finally {
                existFile.delete();
            }
        }
    }
    @Nested
    @DisplayName("calcFileMD5 method test")
    class CalcFileMD5Test{
        @Test
        @SneakyThrows
        @DisplayName("calcFileMD5 method test when parameter class is MultipartFile")
        void testCalcFileMD5WithMultipartFile(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            FileItem fileItem = mock(FileItem.class);
            when(fileItem.getFieldName()).thenReturn("a.jar");
            MockMultipartFile mp = new MockMultipartFile("a.jar","a.jar","",fileItem.getInputStream());
            String md5 = PdkSourceUtils.calcFileMD5(mp);
            assertNotEquals(null,md5);
            assertEquals(32, md5.length());
        }
        @Test
        @SneakyThrows
        @DisplayName("calcFileMD5 method test when parameter class is File")
        void testCalcFileMD5WithFile(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            String md5 = PdkSourceUtils.calcFileMD5(existFile);
            existFile.delete();
            assertNotEquals(null,md5);
        }
        @Test
        @SneakyThrows
        @DisplayName("calcFileMD5 method test when parameter class is File")
        void testCalcFileMD5(){
            Object origin = new Object();
            String md5 = PdkSourceUtils.calcFileMD5(origin);
            assertEquals(null,md5);
        }
    }
    @Nested
    @DisplayName("calculateFileMD5 method test")
    class CalculateFileMD5Test{
        @Test
        @DisplayName("calculateFileMD5 method test when isFile return false")
        void testGetFileMD5WithNotFile(){
            File file = new File("dir");
            file.mkdir();
            String fileMD5 = PdkSourceUtils.calculateFileMD5(file);
            assertEquals(null,fileMD5);
            file.delete();
        }
        @Test
        @SneakyThrows
        @DisplayName("calculateFileMD5 method test when isFile return true")
        void testGetFileMD5WithFile(){
            File file = new File("mysql.jar");
            Files.write(file.toPath(), "test".getBytes(StandardCharsets.UTF_8));
            String fileMD5 = PdkSourceUtils.calculateFileMD5(file);
            file.delete();
            assertEquals("98f6bcd4621d373cade4e832627b4f6", fileMD5);
        }
    }
    @Nested
    class TransformToFileTest{
        @Test
        @SneakyThrows
        @DisplayName("test transformToFile method when file already exist")
        void testTransformToFile(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            AtomicReference<File> file = new AtomicReference<>(existFile);
            AtomicBoolean needDeleteFile = new AtomicBoolean(false);
            FileItem fileItem = mock(FileItem.class);
            when(fileItem.getFieldName()).thenReturn("a.jar");
            MockMultipartFile originFile = new MockMultipartFile("a.jar","a.jar","",fileItem.getInputStream());
            PdkSourceUtils.transformToFile(originFile, (k, v)->{
                file.set((File) k);
                needDeleteFile.set((Boolean) v);
            });
            assertEquals(existFile, file.get());
            assertEquals(false, needDeleteFile.get());
            file.get().delete();
        }
        @Test
        @SneakyThrows
        @DisplayName("test transformToFile method when file already exist")
        void testTransformToFileNotExist(){
            AtomicReference<File> file = new AtomicReference<>();
            AtomicBoolean needDeleteFile = new AtomicBoolean(false);
            FileItem fileItem = mock(FileItem.class);
            when(fileItem.getFieldName()).thenReturn("a.jar");
            MockMultipartFile originFile = new MockMultipartFile("a.jar","a.jar","",fileItem.getInputStream());
            PdkSourceUtils.transformToFile(originFile, (k, v)->{
                file.set((File) k);
                needDeleteFile.set((Boolean) v);
            });
            assertEquals("a.jar", file.get().getName());
            assertEquals(true, needDeleteFile.get());
            file.get().delete();
        }
    }
    @Nested
    class InputStreamToFileTest{
        @Test
        @SneakyThrows
        void testInputStreamToFile(){
            InputStream ins = InputStream.nullInputStream();
            File file = new File("a.jar");
            file.createNewFile();
            assertDoesNotThrow(()->PdkSourceUtils.inputStreamToFile(ins,file));
            file.delete();
        }
    }

    private static String md5Hex(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        return String.format("%032x", new BigInteger(1, digest));
    }
}
