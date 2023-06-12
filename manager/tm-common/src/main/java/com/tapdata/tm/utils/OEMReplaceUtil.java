package com.tapdata.tm.utils;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.tapnode.TapNodeContainer;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.IOUtils;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;

/**
 * @author GavinXiao
 * @description Replace create by Gavin
 * @create 2023/6/12 11:50
 *
 * OEM config util
 **/
public class OEMReplaceUtil {
    private static final String path = "manager/tm/src/main/resources/oem/";

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
        return path + oemType + (fileName.startsWith("/") ? "" : "/") + fileName;
    }

    /**
     * @type function
     * @author Gavin
     * @description 获取当前oem文件
     * @param fileName 文件名称，例如：name.json 或者 /name.json
     * @return File
     * @date 2023/6/12
     * */
    public static File getOEMConfigFile(String fileName) throws FileNotFoundException {
        String configPath = getOEMConfigPath(fileName);
        if (null == configPath) return null;
        //if (!file.exists()) {
        //    throw new FileNotFoundException("Can not found " + fileName + "in /" + oemType());
        //}
        return new File(configPath);
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
        File configFile = getOEMConfigFile(fileName);
        if (null == configFile) return null;
        return FileUtils.openInputStream(configFile);
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
        return System.getProperty("oem", null);
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
            String finalTxt = docTxt.toString();
            for (Map.Entry<String, Object> entry : oemConfig.entrySet()) {
                finalTxt = finalTxt.replaceAll(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return new ByteArrayInputStream(finalTxt.getBytes(StandardCharsets.UTF_8));
        } finally {
            try {
                stream.close();
            } catch (Exception ignore) {}
        }
    }
}
