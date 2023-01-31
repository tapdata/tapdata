package io.tapdata.common.util;

import io.tapdata.common.postman.PostManAnalysis;

import java.util.Map;

public class APIUtil {
    public static final String TYPE_POSTMAN = "POST_MAN";
    public static final String TYPE_API_FOX = "API_FOX";
    public static String ApiType(Map<String,Object> apiJson){
        if (PostManAnalysis.isPostMan(apiJson)) {
            return APIUtil.TYPE_POSTMAN;
        }else {
            return null;
        }
    }
}
