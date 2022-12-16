package io.tapdata.quickapi.support.postman.util;


import io.tapdata.quickapi.support.postman.enums.Symbol;

import java.util.regex.Pattern;

public class ReplaceTagUtil {
    public static String replaceToEmpty(String itemStr){
        return itemStr.replaceAll(Symbol.tags(),"");
    }
    public static String replace(String itemStr){
        if (null == itemStr ) return null;
        Symbol[] values = Symbol.values();
        if (values.length <= 0) return itemStr;
        for (Symbol value : values) {
            itemStr = itemStr.replaceAll(value.tag(),value.to());
        }
        return itemStr;
    }

    public static void main(String[] args) {
//        String regx = ".*(JJK\\[[^\\]]+).*";
//        System.out.println("START[123]JJK[526]".matches(regx));
//        System.out.println(Pattern.compile(regx).matcher("START[123]JJK[526]").group());

        String expireStatus = "body.message=NO AUTH&&body.code=500021";
        String[] propertiesArr = expireStatus.split("\\|\\||&&");
        for (String s : propertiesArr) {
            System.out.println(s);
        }
    }
}
