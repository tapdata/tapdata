package com.tapdata.tm.task.constant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class JsonToRelMigConverter {

    public static void main(String[] args) {
        String jsonFilePath = "/Users/xiao/Downloads/mongoRM/tapdata_group/Task.metadata.json";
        String realmMigrationFilePath = "/Users/xiao/Downloads/mongoRM/tapdata_group/task.relmig";

        convertToRelMig(jsonFilePath, realmMigrationFilePath);
    }

    public static void convertToRelMig(String jsonFilePath, String realmMigrationFilePath) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            // 读取JSON文件
            JsonNode rootNode = objectMapper.readTree(new File(jsonFilePath));

            // 准备写入RelMig文件
            StringBuilder relMigContent = new StringBuilder();
            relMigContent.append("// Realm Migration File\n\n");
            relMigContent.append("var migrations = [\n");

            // 遍历JSON数据中的每个文档
            for (JsonNode jsonNode : rootNode) {
                JsonNode document = objectMapper.readTree(jsonNode.toString());
                if (document.isEmpty()) continue;
                if (!document.has("id")) {
                    continue;
                }
                // 将文档转换为RelMig格式
                relMigContent.append("\t{");
                // 假设'i d'字段作为主键
                relMigContent.append("\"id\": ").append(document.get("id").asText()).append(",");
                // 根据您的数据模型继续添加其他字段
                // 示例：relMigContent.append("\"name\": \"").append(document.get("name").asText()).append("\",");
                // 示例：relMigContent.append("\"age\": ").append(document.get("age").asInt()).append(",");
                relMigContent.append("},\n");
            }

        // 结束RelMig文件
        relMigContent.append("];");

        // 将RelMig内容写入文件
        FileUtils.writeStringToFile(new File(realmMigrationFilePath), relMigContent.toString(), "UTF-8");
        
        System.out.println("Conversion completed successfully.");

    } catch (IOException e) {
        e.printStackTrace();
    }
}
}