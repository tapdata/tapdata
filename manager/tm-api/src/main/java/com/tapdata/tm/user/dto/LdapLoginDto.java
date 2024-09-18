package com.tapdata.tm.user.dto;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class LdapLoginDto {
    private String ldapUrl; // host+端口
    private String bindDN; //用户名+域
    private String password; //密码
    private String baseDN; //根目录
    private boolean sslEnable; //是否启用ssl
}
