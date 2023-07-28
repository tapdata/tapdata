package io.tapdata.sybase.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author GavinXiao
 * @description Big5Ha create by Gavin
 * @create 2023/7/24 17:16
 **/
public class Big5Ha {
    private static final Map<String, Character> charsetMap = new HashMap();
    private final String filePath = "ha_mingliu.map";

    public Map<String, Character> getCharsetMap() {
        return charsetMap;
    }

    public Big5Ha() {
        try {
            synchronized (charsetMap) {
                if (charsetMap.isEmpty()) {
                    synchronized (charsetMap) {
                        try (InputStream inputStream = Big5Ha.class.getClassLoader().getResourceAsStream(filePath)) {
                            if (inputStream == null) {
                                return;
                            }
                            try (InputStreamReader stream = new InputStreamReader(inputStream);
                                 BufferedReader reader = new BufferedReader(stream)) {
                                String line;
                                while ((line = reader.readLine()) != null) {
                                    String[] split = line.split(" ");
                                    Character character = (char) Integer.parseInt(split[1], 10);
                                    charsetMap.put(split[0], character);
                                }
                            }
                        } catch (IOException ignored) {
                        }
                    }
                }
            }
        } finally {
        }
    }
}