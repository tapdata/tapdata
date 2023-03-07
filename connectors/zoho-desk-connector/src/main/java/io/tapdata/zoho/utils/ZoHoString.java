package io.tapdata.zoho.utils;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

public class ZoHoString {
    public static void main(String[] args) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            String name = "null.null.null.null";
            builder.append("null.");
            //System.out.println(fistUpper(name,"."));
        }
        long start = System.currentTimeMillis();
        String s = fistUpper(builder.toString(), ".");
        long end = System.currentTimeMillis();
        System.out.println(s);
        System.out.println(end - start);
    }
    public static String fistUpper(String str,String split){
        if (null == str ) return "";
        if (split == null || "".equals(split)) return str;
        StringBuilder builder = new StringBuilder();
        char[] chr = str.toCharArray();
        for (int index = 0; index < chr.length; index++) {
            char curr = chr[index];
            builder.append(String.valueOf(curr).equals(split)?(index == chr.length - 1? "" : String.valueOf((chr[++index])).toUpperCase()):curr);
        }
        return builder.toString();
    }
}
