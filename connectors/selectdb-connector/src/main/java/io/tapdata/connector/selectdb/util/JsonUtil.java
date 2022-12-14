package io.tapdata.connector.selectdb.util;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.io.*;
import java.util.List;

/**
 * Author:Skeet
 * Date: 2022/12/14
 **/
public class JsonUtil {
    public static void writeLocalJson(
            String filePath, List<TapRecordEvent> list
    ) throws IOException {
        // 保证创建一个新文件
        File file = new File(filePath);
        if (!file.getParentFile().exists()) { // 如果父目录不存在，创建父目录
            file.getParentFile().mkdirs();
        }
        if (file.exists()) { // 如果已存在,删除旧文件
            file.delete();
        }
        file.createNewFile();

        Object json = JSONObject.toJSON(list);

        // 将格式化后的字符串写入文件
        Writer write = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
        write.write(json.toString());
        write.flush();
        write.close();
    }
}
