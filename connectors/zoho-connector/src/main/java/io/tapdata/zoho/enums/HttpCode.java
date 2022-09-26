package io.tapdata.zoho.enums;

import io.tapdata.zoho.utils.Checker;

public enum HttpCode {
    SUCCEED("SUCCEED","SUCCEED"),
    ERROR("ERROR","ERROR"),
    INVALID_OAUTH("INVALID_OAUTH","The OAuth Token you provided is invalid."),
    EMPTY("EMPTY","HTTP connect failed."),
    GENERAL_ERROR("general_error","General error."),
    INVALID_CODE("invalid_code","Invalid generate code."),
    INVALID_CLIENT("invalid_client","Invalid client ID."),
    INVALID_CLIENT_SECRET("invalid_client_secret","Invalid client secret ID."),
    ;
    String code;
    String message;
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
}
