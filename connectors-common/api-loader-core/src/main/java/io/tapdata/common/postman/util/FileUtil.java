package io.tapdata.common.postman.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Objects;

public class FileUtil {
    public static boolean fileExists(String path){
        return new File(path).exists();
    }
    public static String readString(String path) {
        File file = new File(path);
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String s = null;
            while (Objects.nonNull((s = br.readLine()))) {
                result.append(System.lineSeparator()).append(s);
            }
            br.close();
        } catch (Exception ignored) {
        }
        return result.toString();
    }
}
