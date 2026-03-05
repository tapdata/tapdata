package com.tapdata.tm.utils;

import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.*;

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
        @DisplayName("getFileMD5 should use multipart content even if file exists")
        void testGetFileMD5UsesMultipartContentWhenFileExists(){
            File existFile = new File("a.jar");
            Files.write(existFile.toPath(), "old".getBytes(StandardCharsets.UTF_8));
            MockMultipartFile mp = new MockMultipartFile(
                    "a.jar",
                    "a.jar",
                    "application/octet-stream",
                    "new".getBytes(StandardCharsets.UTF_8)
            );
            String md5 = PdkSourceUtils.getFileMD5(mp);
            existFile.delete();
            assertEquals(md5Hex("new".getBytes(StandardCharsets.UTF_8)), md5);
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
        @DisplayName("calcFileMD5 method test when parameter class is MultipartFile")
        void testCalcFileMD5WithMultipartFile(){
            MockMultipartFile mp = new MockMultipartFile(
                    "a.jar",
                    "a.jar",
                    "application/octet-stream",
                    "content".getBytes(StandardCharsets.UTF_8)
            );
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
            assertEquals("098f6bcd4621d373cade4e832627b4f6", fileMD5);
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
            MockMultipartFile originFile = new MockMultipartFile(
                    "a.jar",
                    "a.jar",
                    "application/octet-stream",
                    "content".getBytes(StandardCharsets.UTF_8)
            );
            PdkSourceUtils.transformToFile(originFile, (k, v)->{});
            existFile.delete();
        }
        @Test
        @SneakyThrows
        @DisplayName("test transformToFile method when file already exist")
        void testTransformToFileNotExist(){
            MockMultipartFile originFile = new MockMultipartFile(
                    "a.jar",
                    "a.jar",
                    "application/octet-stream",
                    "content".getBytes(StandardCharsets.UTF_8)
            );
            PdkSourceUtils.transformToFile(originFile, (k, v)->{});
            File file = new File("a.jar");
            if (file.exists()) {
                file.delete();
            }
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
