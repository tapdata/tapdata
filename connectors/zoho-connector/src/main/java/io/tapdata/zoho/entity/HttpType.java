package io.tapdata.zoho.entity;

import io.tapdata.zoho.utils.Checker;

public enum HttpType {
    POST("POST"),
    GET("GET"),
    DELETE("DELETE"),
    PATCH("PATCH"),
    PUT("PUT");
    String type;
    HttpType(String type){
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    public static HttpType set(String typeName){
        if (Checker.isEmpty(typeName)) return POST;
        HttpType[] values = HttpType.values();
        for (HttpType value : values) {
            if (value.type.equals(typeName)){
                return value;
            }
        }
        return POST;
    }
}
