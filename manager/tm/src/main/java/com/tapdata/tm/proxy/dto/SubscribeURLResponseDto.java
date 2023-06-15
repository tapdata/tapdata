package com.tapdata.tm.proxy.dto;

/**
 * @author GavinXiao
 * @description SubscribeURLResponseDto create by Gavin
 * @create 2023/6/14 15:17
 **/
public class SubscribeURLResponseDto extends SubscribeResponseDto {
    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

}
