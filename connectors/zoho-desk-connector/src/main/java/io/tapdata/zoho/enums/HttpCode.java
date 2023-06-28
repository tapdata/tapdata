package io.tapdata.zoho.enums;

import io.tapdata.zoho.annonation.LanguageEnum;
import io.tapdata.zoho.utils.Checker;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public enum HttpCode {
    SUCCEED("SUCCEED","Succeed"),
    ERROR("ERROR","Error"),
    INVALID_OAUTH("INVALID_OAUTH","The OAuth Token you provided is invalid"),
    EMPTY("EMPTY","HTTP connect failed"),
    GENERAL_ERROR("general_error","General error"),
    INVALID_CODE("invalid_code","Invalid generate code"),
    INVALID_CLIENT("invalid_client","Invalid client ID"),
    INVALID_CLIENT_SECRET("invalid_client_secret","Invalid client secret"),
    ;
    String code;
    String message;
    public static Map<String,Map<String,String>> langMap = new HashMap<String,Map<String,String>>(){{
        put(LanguageEnum.EN.getLanguage(),new HashMap<String,String>(){{
            put(SUCCEED.getCode(),              SUCCEED.getMessage());
            put(ERROR.getCode(),                ERROR.getMessage());
            put(INVALID_OAUTH.getCode(),        INVALID_OAUTH.getMessage());
            put(EMPTY.getCode(),                EMPTY.getMessage());
            put(GENERAL_ERROR.getCode(),        GENERAL_ERROR.getMessage());
            put(INVALID_CODE.getCode(),         INVALID_CODE.getMessage());
            put(INVALID_CLIENT.getCode(),       INVALID_CLIENT.getMessage());
            put(INVALID_CLIENT_SECRET.getCode(),INVALID_CLIENT_SECRET.getMessage());
        }});
        put(LanguageEnum.ZH_CN.getLanguage(),new HashMap<String,String>(){{
            put(SUCCEED.getCode(),              "获取成功");
            put(ERROR.getCode(),                "获取失败");
            put(INVALID_OAUTH.getCode(),        "您提供的连接信息已过期");
            put(EMPTY.getCode(),                "连接失败");
            put(GENERAL_ERROR.getCode(),        "操作失败");
            put(INVALID_CODE.getCode(),         "生成代码已过期，请前往重新获取");
            put(INVALID_CLIENT.getCode(),       "客户端ID码已过期，请前往重新获取");
            put(INVALID_CLIENT_SECRET.getCode(),"客户端机密码已过期，请前往重新获取");
        }});
        put(LanguageEnum.ZH_TW.getLanguage(),new HashMap<String,String>(){{
            put(SUCCEED.getCode(),              "獲取成功");
            put(ERROR.getCode(),                "獲取失敗");
            put(EMPTY.getCode(),                "連接失敗");
            put(INVALID_OAUTH.getCode(),        "您提供的連接資訊已過期");
            put(GENERAL_ERROR.getCode(),        "操作失敗");
            put(INVALID_CODE.getCode(),         "生成程式碼已過期，請前往重新獲取");
            put(INVALID_CLIENT.getCode(),       "用戶端ID碼已過期，請前往重新獲取");
            put(INVALID_CLIENT_SECRET.getCode(),"用戶端機密碼已過期，請前往重新獲取");
        }});
    }};
    HttpCode(String code,String message){
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public static HttpCode code(String code){
        if (Checker.isEmpty(code)) return null;
        HttpCode[] values = values();
        for (HttpCode value : values) {
            if (value.getCode().equals(code)) return value;
        }
        return null;
    }
    public static String message(String language,String code){
        Map<String, String> stringStringMap = HttpCode.langMap.get(language);
        if(null == stringStringMap || stringStringMap.isEmpty()){
            stringStringMap = HttpCode.langMap.get(LanguageEnum.EN.getLanguage());
        }
        return new String(stringStringMap.get(code).getBytes(), StandardCharsets.UTF_8);
    }
}
