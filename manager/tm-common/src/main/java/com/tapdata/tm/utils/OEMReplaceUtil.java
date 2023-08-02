package com.tapdata.tm.utils;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import org.springframework.core.io.ClassPathResource;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

/**
 * @author GavinXiao
 * @description Replace create by Gavin
 * @create 2023/6/12 11:50
 *
 * OEM config util
 **/
public class OEMReplaceUtil {
    private static final String path = "oem/";

    /**
     * @type function
     * @author Gavin
     * @description 获取当前oem文件的相对路径
     * @param fileName 文件名称，例如：name.json 或者 /name.json
     * @return String
     * @date 2023/6/12
     * */
    public static String getOEMConfigPath(String fileName){
        String oemType = oemType();
        if (null == fileName || "".equals(fileName.trim()) || null == oemType) return null;
        return Optional.ofNullable(System.getenv("oem_file_path")).orElse(path) + oemType + (fileName.startsWith("/") ? "" : "/") + fileName;
    }

    /**
     * @type function
     * @author Gavin
     * @description 获取当前oem文件
     * @param fileName 文件名称，例如：name.json 或者 /name.json
     * @return File
     * @date 2023/6/12
     * */
    public static InputStream getOEMConfigInputStream(String fileName) throws FileNotFoundException {
        String configPath = getOEMConfigPath(fileName);
        if (null == configPath) return null;
        //if (!file.exists()) {
        //    throw new FileNotFoundException("Can not found " + fileName + "in /" + oemType());
        //}
        try {
            ClassPathResource classPathResource = new ClassPathResource(configPath);
            return classPathResource.getInputStream();
            //return org.springframework.util.ResourceUtils.getFile(configPath);//new File(configPath);
        }catch (Exception e){
            //TapLogger.info("FileNotFound in path {}", configPath);
            TapLogger.warn("OEM-REPLACE", "OEM name is {} in evn, but file not found in path {}", oemType(), configPath);
            //throw new CoreException("OEM name is {} in evn, but file not found in path {}", oemType(), configPath);
        }
        return null;
    }

    /**
     * @type function
     * @author Gavin
     * @description 获取当前oem文件流
     * @param fileName 文件名称，例如：name.json 或者 /name.json
     * @return InputStream
     * @date 2023/6/12
     * */
    public static InputStream getOEMConfigFileAsInputStream(String fileName) throws IOException {
        assertFileName(fileName);
        return getOEMConfigInputStream(fileName);
    }

    /**
     * @type function
     * @author Gavin
     * @description 获取当前类型为JSON的oem文件，并转为Map
     * @param fileName 文件名称，例如：name.json 或者 /name.json
     * @return Map
     * @date 2023/6/12
     * */
    public static Map<String, Object> getOEMConfigMap(String fileName){
        try {
            InputStream inputStream = getOEMConfigFileAsInputStream(fileName);
            if (null == inputStream) return null;
            return JSON.parseObject(inputStream, StandardCharsets.UTF_8, LinkedHashMap.class);
        }catch (IOException e){}
        return null;
    }

    private static void assertFileName(String fileName) {
        if (null == fileName || "".equals(fileName.trim())) {
            throw new IllegalArgumentException("File name can not be empty");
        }
    }

    /**
     * @type function
     * @author Gavin
     * @description 获取当前oem类型
     * @return String
     * @date 2023/6/12
     * */
    public static String oemType() {
        return System.getenv("oem");
    }

    /**
     * @type function
     * @author Gavin
     * @description 根据对应的OEM配置，替换流中的相应关键字，输出新的流
     * @param fileName oem配置文件名称
     * @param stream 文件流
     * @return InputStream
     * @date 2023/6/12
     * */
    public static InputStream replace(InputStream stream, String fileName){
        Map<String, Object> replaceConfig = getOEMConfigMap(fileName);
        return replace(stream, replaceConfig);
    }

    /**
     * @type function
     * @author Gavin
     * @description 根据对应的OEM配置，替换流中的相应关键字，输出新的流
     * @param oemConfig oem配置
     * @param stream 文件流
     * @return InputStream
     * @date 2023/6/12
     * */
    public static InputStream replace(InputStream stream, Map<String, Object> oemConfig){
        if (null == oemConfig || oemConfig.isEmpty() || null == stream) return stream;
        try (Scanner scanner = new Scanner(stream, "UTF-8")) {
            StringBuilder docTxt = new StringBuilder();
            while (scanner.hasNextLine()) {
                docTxt.append(scanner.nextLine()).append("\n");
            }
            String finalTxt = replace(docTxt.toString(), oemConfig);
            return new ByteArrayInputStream(finalTxt.getBytes(StandardCharsets.UTF_8));
        } finally {
            try {
                stream.close();
            } catch (Exception ignore) {}
        }
    }

    public static String replace(String item, String fileName){
        Map<String, Object> replaceConfig = getOEMConfigMap(fileName);
        return replace(item, replaceConfig);
    }

    public static String replace(String item, Map<String, Object> oemConfig) {
        if (null == oemConfig || oemConfig.isEmpty() || null == item || "".equals(item.trim())) return item;
        for (Map.Entry<String, Object> entry : oemConfig.entrySet()) {
            String key = entry.getKey();
            if (null == key) continue;
            item = item.replaceAll(key, String.valueOf(entry.getValue()));
        }
        return item;
    }
}
