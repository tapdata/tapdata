package com.tapdata.tm.utils;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
public class TMStartMsgUtil {
    private static String fullPath = ".tmStartMsg.json";
    public static void writeTMStartMsg(TmStartMsg tmStartMsg){
            // 保证创建一个新文件
            if(StringUtils.isNotEmpty(System.getenv("TAPDATA_WORK_DIR"))){
                fullPath = System.getenv("TAPDATA_WORK_DIR") + File.separator + fullPath;
            }
            File file = new File(fullPath);
            if (file.exists()) {
                try {
                    Files.delete(Paths.get(fullPath));
                }catch (Exception e){
                    log.warn("Failed to delete .tmStartMsg.json:{}",e.getMessage());
                }
            }
            try {
                file.createNewFile();
            }catch (Exception e){
                log.warn("Failed to create .tmStartMsg.json:{}",e.getMessage());
            }
            String jsonString = JSON.toJSONString(tmStartMsg);
            // 格式化json字符串
            // 将格式化后的字符串写入文件
            try (Writer write = new OutputStreamWriter(Files.newOutputStream(file.toPath()), "UTF-8");){
                write.write(jsonString);
                write.flush();
            } catch (Exception e) {
                log.warn("Failed to write .tmStartMsg.json:{}",e.getMessage());
            }
    }
}
