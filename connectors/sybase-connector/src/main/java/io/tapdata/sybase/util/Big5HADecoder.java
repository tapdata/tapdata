package io.tapdata.sybase.util;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class Big5HADecoder {
    public static final Big5Ha context = new Big5Ha();
    private Map<String, Character> characterHashMap = new HashMap<>();

    public static String getMKey(byte[] b) {
        StringBuilder mKey = new StringBuilder();
        for (byte bb: b) {
            mKey.append(bb).append("_");
        }
        return mKey.toString();
    }

    public Big5HADecoder(String encoding) {
        byte[] b;
        for (String key: context.getCharsetMap().keySet()) {
            b = key.getBytes(Charset.forName(encoding));
            String mKey = getMKey(b);
            characterHashMap.put(mKey, context.getCharsetMap().get(key));
        }
    }

    // 1. 如果首位是 0-255, 则认为是单字符, 直接加上去
    // 2. 如果不是, 则为双字符, 使用 map => big5hkscs 优先级解码
    public String decode(byte[] bytes) {
        // 这是最终结果
        StringBuilder decodeString = new StringBuilder();

        // 索引 key
        byte[] bb = new byte[2];
        char c;
        for (int i=0; i<bytes.length; i++) {
            // 最后一个字符, 直接加上就可以, 不需要 decode
            if (i == bytes.length - 1) {
                c = (char) bytes[i];
                decodeString.append(c);
                break;
            }

            // 查找当前双字符是否在 map 里
            bb[0] = bytes[i];
            bb[1] = bytes[i+1];
            // 0-255 之间的字符, 是标准单字节头, 加上去
            if (bb[0] > 0 && bb[0] < 255) {
                c = (char) bb[0];
                decodeString.append(c);
                continue;
            }

            // 这里必然是双字符编码, i++
            i++;
            if (characterHashMap.containsKey(getMKey(bb))) {
                decodeString.append(characterHashMap.get(getMKey(bb)));
            } else {
                decodeString.append(new String(bb, Charset.forName("big5hkscs")));
            }
        }
        return decodeString.toString();
    }
}