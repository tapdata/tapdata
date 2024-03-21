package com.tapdata.tm.utils;

import lombok.SneakyThrows;
import org.apache.commons.fileupload.FileItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import java.io.File;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PdkSourceUtilsTest {
    @Nested
    @DisplayName("getFileMD5 method test")
    class GetFileMD5Test{
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test when parameter class is CommonsMultipartFile")
        void testGetFileMD5WithCommonsMultipartFile(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            FileItem fileItem = mock(FileItem.class);
            when(fileItem.getFieldName()).thenReturn("a.jar");
            CommonsMultipartFile originFile = new CommonsMultipartFile(fileItem);
            String md5 = PdkSourceUtils.getFileMD5(originFile);
            existFile.delete();
            assertNotEquals(null,md5);
        }
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test when parameter class is File")
        void testGetFileMD5WithFile(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            String md5 = PdkSourceUtils.getFileMD5(existFile);
            existFile.delete();
            assertNotEquals(null,md5);
        }
    }
    @Nested
    @DisplayName("calcFileMD5 method test")
    class CalcFileMD5Test{
        @Test
        @SneakyThrows
        @DisplayName("calcFileMD5 method test when parameter class is CommonsMultipartFile")
        void testCalcFileMD5WithCommonsMultipartFile(){
            File existFile = new File("a.jar");
            existFile.createNewFile();
            FileItem fileItem = mock(FileItem.class);
            when(fileItem.getFieldName()).thenReturn("a.jar");
            CommonsMultipartFile originFile = new CommonsMultipartFile(fileItem);
            String md5 = PdkSourceUtils.calcFileMD5(originFile);
            existFile.delete();
            assertNotEquals(null,md5);
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
            File file = mock(File.class);
            String fileMD5 = PdkSourceUtils.calculateFileMD5(file);
            assertEquals(null,fileMD5);
        }
        @Test
        @SneakyThrows
        @DisplayName("calculateFileMD5 method test when isFile return true")
        void testGetFileMD5WithFile(){
            File file = new File("mysql.jar");
            file.createNewFile();
            String fileMD5 = PdkSourceUtils.calculateFileMD5(file);
            file.delete();
            assertNotEquals(null,fileMD5);
        }
        @Test
        @SneakyThrows
        @DisplayName("calculateFileMD5 method test with exception")
        void testGetFileMD5WithEx(){
            try (MockedStatic<MessageDigest> mb = Mockito
                    .mockStatic(MessageDigest.class)) {
                mb.when(()->MessageDigest.getInstance("MD5")).thenThrow(new NoSuchAlgorithmException("mock NoSuchAlgorithmException"));
                File file = new File("mysql.jar");
                file.createNewFile();
                String fileMD5 = PdkSourceUtils.calculateFileMD5(file);
                file.delete();
                assertEquals(null,fileMD5);}
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
            CommonsMultipartFile originFile = new CommonsMultipartFile(fileItem);
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
            CommonsMultipartFile originFile = new CommonsMultipartFile(fileItem);
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
            InputStream ins = mock(InputStream.class);
            when(ins.read(any())).thenReturn(-1);
            File file = new File("a.jar");
            file.createNewFile();
            assertDoesNotThrow(()->PdkSourceUtils.inputStreamToFile(ins,file));
            file.delete();
        }
    }
}
