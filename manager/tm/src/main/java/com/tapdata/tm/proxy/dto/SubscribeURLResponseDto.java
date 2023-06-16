package com.tapdata.tm.proxy.dto;

/**
 * @author GavinXiao
 * @description SubscribeURLResponseDto create by Gavin
 * @create 2023/6/14 15:17
 **/
public class SubscribeURLResponseDto extends SubscribeResponseDto {
    private String path;

    private String access_token;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getAccess_token() {
        return access_token;
    }

    public void setAccess_token(String access_token) {
        this.access_token = access_token;
    }
}
