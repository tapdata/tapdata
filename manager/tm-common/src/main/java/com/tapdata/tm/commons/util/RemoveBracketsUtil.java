package com.tapdata.tm.commons.util;

public class RemoveBracketsUtil {

    public static  String removeBrackets  (String content){
        if(content.contains("(") || content.contains(")")){
            // 去除英文括号及内容
            content=content.replaceAll("[\\[][^\\[\\]]+[\\]]|[\\(][^\\(\\)]+[\\)]", "");
        }
        if(content.contains("（") || content.contains("）")){
            // 去除中文括号及内容
            content=content.replaceAll("[\\[][^\\[\\]]+[\\]]|[\\（)][^\\(\\)]+[\\）]", "");
        }
        return content;
    }
}
