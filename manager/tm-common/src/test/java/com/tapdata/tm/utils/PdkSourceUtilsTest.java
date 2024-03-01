package com.tapdata.tm.utils;

import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;

public class PdkSourceUtilsTest {
    @Nested
    @DisplayName("getFileMD5 method test")
    class getFileMD5Test{
        @Test
        @DisplayName("getFileMD5 method test when isFile return false")
        void testGetFileMD5WithNotFile(){
            File file = mock(File.class);
            String fileMD5 = PdkSourceUtils.getFileMD5(file);
            assertEquals(null,fileMD5);
        }
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test when isFile return true")
        void testGetFileMD5WithFile(){
            File file = new File("mysql.jar");
            file.createNewFile();
            String fileMD5 = PdkSourceUtils.getFileMD5(file);
            file.delete();
            assertNotEquals(null,fileMD5);
        }
        @Test
        @SneakyThrows
        @DisplayName("getFileMD5 method test with exception")
        void testGetFileMD5WithEx(){
            try (MockedStatic<MessageDigest> mb = Mockito
                    .mockStatic(MessageDigest.class)) {
                mb.when(()->MessageDigest.getInstance("MD5")).thenThrow(new NoSuchAlgorithmException("mock NoSuchAlgorithmException"));
                File file = new File("mysql.jar");
                file.createNewFile();
                String fileMD5 = PdkSourceUtils.getFileMD5(file);
                file.delete();
                assertEquals(null,fileMD5);}
        }
    }
}
