package com.tapdata.tm.utils;

import com.alibaba.fastjson.JSON;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class TMStartMsgUtil {
    private static String fullPath =  File.separator + ".tmStartMsg.json";
    public static void writeTMStartMsg(TmStartMsg tmStartMsg){
        try {
            // 保证创建一个新文件
            if(null != System.getenv("TAPDATA_WORK_DIR")){
                fullPath = System.getenv("TAPDATA_WORK_DIR") + fullPath;
            }
            File file = new File(fullPath);
            if (file.exists()) { // 如果已存在,删除旧文件
                file.delete();
            }
            file.createNewFile();
            String jsonString = JSON.toJSONString(tmStartMsg);
            // 格式化json字符串
            // 将格式化后的字符串写入文件
            Writer write = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            write.write(jsonString);
            write.flush();
            write.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
