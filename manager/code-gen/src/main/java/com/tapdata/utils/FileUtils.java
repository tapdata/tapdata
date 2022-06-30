package com.tapdata.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class FileUtils {

    public static String readFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), StandardCharsets.UTF_8);
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    public static String readResourceFile(String fileName) {
        String jsonStr = "";
        try {
            InputStream resourceAsStream = FileUtils.class.getClassLoader().getResourceAsStream(fileName);
            assert resourceAsStream != null;
            BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAsStream));
            int ch;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
