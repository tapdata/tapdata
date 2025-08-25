package com.tapdata.tm.init;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SSLFileUtilTest {

    @TempDir
    Path tempDir;

    @Test
    void testReadFileAsString() throws IOException {
        // 创建测试文件
        File testFile = tempDir.resolve("test.txt").toFile();
        String testContent = "Hello\nWorld\nTest Content";
        
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write(testContent);
        }

        // 测试读取文件
        String result = SSLFileUtil.readFileAsString(testFile.getAbsolutePath());
        assertNotNull(result);
        assertEquals(testContent, result);
    }

    @Test
    void testReadFileAsStringWithNullPath() {
        String result = SSLFileUtil.readFileAsString(null);
        assertNull(result);
    }

    @Test
    void testReadFileAsStringWithEmptyPath() {
        String result = SSLFileUtil.readFileAsString("");
        assertNull(result);
    }

    @Test
    void testReadFileAsStringWithNonExistentFile() {
        String result = SSLFileUtil.readFileAsString("/path/to/nonexistent/file.txt");
        assertNull(result);
    }

    @Test
    void testIsValidCertificate() {
        String validCert = "-----BEGIN CERTIFICATE-----\nMIIC...content...\n-----END CERTIFICATE-----";
        assertTrue(SSLFileUtil.isValidCertificate(validCert));

        String invalidCert = "This is not a certificate";
        assertFalse(SSLFileUtil.isValidCertificate(invalidCert));

        assertFalse(SSLFileUtil.isValidCertificate(null));
        assertFalse(SSLFileUtil.isValidCertificate(""));
    }

    @Test
    void testIsValidPrivateKey() {
        String validKey1 = "-----BEGIN PRIVATE KEY-----\nMIIE...content...\n-----END PRIVATE KEY-----";
        assertTrue(SSLFileUtil.isValidPrivateKey(validKey1));

        String validKey2 = "-----BEGIN RSA PRIVATE KEY-----\nMIIE...content...\n-----END RSA PRIVATE KEY-----";
        assertTrue(SSLFileUtil.isValidPrivateKey(validKey2));

        String validKey3 = "-----BEGIN EC PRIVATE KEY-----\nMIIE...content...\n-----END EC PRIVATE KEY-----";
        assertTrue(SSLFileUtil.isValidPrivateKey(validKey3));

        String invalidKey = "This is not a private key";
        assertFalse(SSLFileUtil.isValidPrivateKey(invalidKey));

        assertFalse(SSLFileUtil.isValidPrivateKey(null));
        assertFalse(SSLFileUtil.isValidPrivateKey(""));
    }

    @Test
    void testReadAndValidateCertificate() throws IOException {
        // 创建有效的证书文件
        File certFile = tempDir.resolve("cert.pem").toFile();
        String validCertContent = "-----BEGIN CERTIFICATE-----\nMIIC...valid certificate content...\n-----END CERTIFICATE-----";
        
        try (FileWriter writer = new FileWriter(certFile)) {
            writer.write(validCertContent);
        }

        String result = SSLFileUtil.readAndValidateCertificate(certFile.getAbsolutePath());
        assertNotNull(result);
        assertEquals(validCertContent, result);

        // 测试无效证书文件
        File invalidCertFile = tempDir.resolve("invalid_cert.pem").toFile();
        try (FileWriter writer = new FileWriter(invalidCertFile)) {
            writer.write("This is not a valid certificate");
        }

        String invalidResult = SSLFileUtil.readAndValidateCertificate(invalidCertFile.getAbsolutePath());
        assertNull(invalidResult);
    }

    @Test
    void testReadAndValidatePrivateKey() throws IOException {
        // 创建有效的私钥文件
        File keyFile = tempDir.resolve("key.pem").toFile();
        String validKeyContent = "-----BEGIN PRIVATE KEY-----\nMIIE...valid private key content...\n-----END PRIVATE KEY-----";
        
        try (FileWriter writer = new FileWriter(keyFile)) {
            writer.write(validKeyContent);
        }

        String result = SSLFileUtil.readAndValidatePrivateKey(keyFile.getAbsolutePath());
        assertNotNull(result);
        assertEquals(validKeyContent, result);

        // 测试无效私钥文件
        File invalidKeyFile = tempDir.resolve("invalid_key.pem").toFile();
        try (FileWriter writer = new FileWriter(invalidKeyFile)) {
            writer.write("This is not a valid private key");
        }

        String invalidResult = SSLFileUtil.readAndValidatePrivateKey(invalidKeyFile.getAbsolutePath());
        assertNull(invalidResult);
    }

    @Test
    void testReadFileAsStringNIO() throws IOException {
        // 创建测试文件
        File testFile = tempDir.resolve("test_nio.txt").toFile();
        String testContent = "Hello\nWorld\nNIO Test Content";
        
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write(testContent);
        }

        // 测试使用 NIO 读取文件
        String result = SSLFileUtil.readFileAsStringNIO(testFile.getAbsolutePath());
        assertNotNull(result);
        assertEquals(testContent, result);
    }
}
