package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import java.io.UnsupportedEncodingException;

public class CharsetUtil {
    public CharsetUtil() {
    }

    public static String decode(String str, String fromCharset, String toCharset) throws UnsupportedEncodingException {
        return null == str ? null : new String(str.getBytes(fromCharset), toCharset);
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        String[] latin1Strings = {"½¯½­ÚS", "Ñî•i","ÀîÆMÏ£","Åá«häü"};
        System.out.println(CharsetUtil.decode(latin1Strings[0], "Latin1", "GB18030"));
    }
}