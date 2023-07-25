package io.tapdata.sybase.util;

import java.nio.charset.Charset;
import java.util.Arrays;
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

    public String decode(byte[] bytes) {
        // 新建一个待解析的 buffer
        byte [] buffer = new byte[bytes.length];

        // 初始化 index
        int bufferI = -1;

        // 这是最终结果
        StringBuilder decodeString = new StringBuilder();

        // 索引 key
        byte[] bb = new byte[2];
        for (int i=0; i<bytes.length; i++) {
            // 最后一个字符, 直接加上就可以, 不需要 decode
            if (i == bytes.length - 1) {
                bufferI += 1;
                buffer[bufferI] = bytes[i];
                break;
            }

            // 查找当前双字符是否在 map 里
            bb[0] = bytes[i];
            bb[1] = bytes[i+1];
            if (characterHashMap.containsKey(getMKey(bb))) {
                // 如果是, 则将 buffer 里的内容进行解析, 并加上 map 的内容
                if (bufferI > -1) {
                    decodeString.append(new String(Arrays.copyOfRange(buffer, 0, bufferI+1), Charset.forName("big5hkscs")));
                    bufferI = -1;
                }
                decodeString.append(characterHashMap.get(getMKey(bb)));
                // 由于这里已经加上了双字符的映射, 所以 i 需要 +1
                i++;
            } else {
                // 如果不是, 则将当前字符加入 buffer
                bufferI += 1;
                buffer[bufferI] = bytes[i];
            }
        }

        // 如果 buffer 里还有内容, 则进行一次解码
        if (bufferI > -1) {
            decodeString.append(new String(Arrays.copyOfRange(buffer, 0, bufferI+1), Charset.forName("big5hkscs")));
        }
        return decodeString.toString();
    }
}