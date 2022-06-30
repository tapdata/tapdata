package com.tapdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.config.Config;
import com.tapdata.generator.Generator;
import com.tapdata.generator.GeneratorFactory;
import com.tapdata.utils.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class Main {
    private final static ObjectMapper objectMapper = new ObjectMapper();

    //打jar包需要用到的main
//    public static void main(String[] args) throws Exception {
//        String jsonPath = args[0];
//        String targetPath = args[1];
//        String packageName = null;
//        if (args.length > 2) {
//            packageName = args[2];
//        }
//        gen(jsonPath, targetPath, packageName);
//    }


    public static void main(String[] args) throws Exception {
        gen("D:\\tapDataCode\\drs\\tm\\code-gen\\src\\main\\resources\\schma.json",
                "D:\\tapDataCode\\drs\\tm\\tm\\src\\main\\java\\com\\tapdata\\tm\\log");
    }


    private static void gen(String jsonPath, String targetPath) throws IOException {
        gen(jsonPath, targetPath, null);
    }

    private static void gen(String jsonPath, String targetPath, String packageName) throws IOException {
        String json = FileUtils.readFile(jsonPath);


        LinkedHashMap<String, Object> hashMap = objectMapper.readValue(json, LinkedHashMap.class);
        Config config = new Config();
        config.setPath(targetPath);
        if (packageName == null || packageName.length() == 0) {
            config.setPackageName(getPackageByFilePath(targetPath));
        } else {
            config.setPackageName(packageName);
        }
        config.setBaseClassName("Tapdata");
        config.setBasePackageName("com.tapdata.tm.base");
        String type = (String) hashMap.get("type");
        String title = (String) hashMap.get("title");
        Generator generator = GeneratorFactory.getGenerator(type);
        generator.write(null, config, hashMap, title);
    }

    private static String getPackageByFilePath(String filePath) {
        filePath = filePath.replace("\\", "/");
        int index = filePath.indexOf("java");
        String substring = filePath.substring(index + 5);
        String packageName = substring.replace("/", ".");
        if (packageName.endsWith(".")) {
            packageName = packageName.substring(0, packageName.length() - 1);
        }
        return packageName;
    }

}
