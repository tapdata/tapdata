package com.tapdata.tm.init;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * SSL 证书文件读取工具类
 */
@Slf4j
public class SSLFileUtil {
    
    /**
     * 读取文件内容并转换为字符串（使用 BufferedReader）
     * @param filePath 文件路径
     * @return 文件内容字符串，如果读取失败返回 null
     */
    public static String readFileAsString(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            log.warn("File path is blank");
            return null;
        }
        
        File file = new File(filePath);
        if (!file.exists()) {
            log.warn("File does not exist: {}", filePath);
            return null;
        }
        
        if (!file.isFile()) {
            log.warn("Path is not a file: {}", filePath);
            return null;
        }
        
        if (!file.canRead()) {
            log.warn("File is not readable: {}", filePath);
            return null;
        }
        
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append(System.lineSeparator());
            }
            // 移除最后一个换行符
            if (content.length() > 0) {
                content.setLength(content.length() - System.lineSeparator().length());
            }
            
            log.debug("Successfully read file: {}, content length: {}", filePath, content.length());
            return content.toString();
            
        } catch (IOException e) {
            log.error("Failed to read file: {}, error: {}", filePath, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 读取文件内容并转换为字符串（使用 NIO）
     * @param filePath 文件路径
     * @return 文件内容字符串，如果读取失败返回 null
     */
    public static String readFileAsStringNIO(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            log.warn("File path is blank");
            return null;
        }
        
        try {
            Path path = Paths.get(filePath);
            
            if (!Files.exists(path)) {
                log.warn("File does not exist: {}", filePath);
                return null;
            }
            
            if (!Files.isRegularFile(path)) {
                log.warn("Path is not a regular file: {}", filePath);
                return null;
            }
            
            if (!Files.isReadable(path)) {
                log.warn("File is not readable: {}", filePath);
                return null;
            }
            
            byte[] bytes = Files.readAllBytes(path);
            String content = new String(bytes, StandardCharsets.UTF_8);
            
            log.debug("Successfully read file using NIO: {}, content length: {}", filePath, content.length());
            return content;
            
        } catch (IOException e) {
            log.error("Failed to read file using NIO: {}, error: {}", filePath, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 验证证书文件格式
     * @param content 证书内容
     * @return 是否为有效的证书格式
     */
    public static boolean isValidCertificate(String content) {
        if (StringUtils.isBlank(content)) {
            return false;
        }
        
        return content.contains("-----BEGIN CERTIFICATE-----") && 
               content.contains("-----END CERTIFICATE-----");
    }
    
    /**
     * 验证私钥文件格式
     * @param content 私钥内容
     * @return 是否为有效的私钥格式
     */
    public static boolean isValidPrivateKey(String content) {
        if (StringUtils.isBlank(content)) {
            return false;
        }
        
        return (content.contains("-----BEGIN PRIVATE KEY-----") && content.contains("-----END PRIVATE KEY-----")) ||
               (content.contains("-----BEGIN RSA PRIVATE KEY-----") && content.contains("-----END RSA PRIVATE KEY-----")) ||
               (content.contains("-----BEGIN EC PRIVATE KEY-----") && content.contains("-----END EC PRIVATE KEY-----"));
    }
    
    /**
     * 读取并验证证书文件
     * @param filePath 证书文件路径
     * @return 证书内容，如果无效返回 null
     */
    public static String readAndValidateCertificate(String filePath) {
        String content = readFileAsString(filePath);
        if (content != null && isValidCertificate(content)) {
            log.info("Successfully read and validated certificate file: {}", filePath);
            return content;
        } else {
            log.warn("Invalid certificate file format: {}", filePath);
            return null;
        }
    }
    
    /**
     * 读取并验证私钥文件
     * @param filePath 私钥文件路径
     * @return 私钥内容，如果无效返回 null
     */
    public static String readAndValidatePrivateKey(String filePath) {
        String content = readFileAsString(filePath);
        if (content != null && isValidPrivateKey(content)) {
            log.info("Successfully read and validated private key file: {}", filePath);
            return content;
        } else {
            log.warn("Invalid private key file format: {}", filePath);
            return null;
        }
    }
}
