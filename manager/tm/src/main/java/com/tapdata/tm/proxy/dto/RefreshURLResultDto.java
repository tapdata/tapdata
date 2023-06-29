package com.tapdata.tm.proxy.dto;

/**
 * @author GavinXiao
 * @description RefreshURLResultDto create by Gavin
 * @create 2023/6/15 11:41
 **/
public class RefreshURLResultDto extends SubscribeURLResponseDto {
    private String host;
    private String fullPath;

    public String getFullPath() {
        return fullPath;
    }

    public void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
