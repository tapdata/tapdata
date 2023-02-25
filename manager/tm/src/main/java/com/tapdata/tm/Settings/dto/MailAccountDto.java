package com.tapdata.tm.Settings.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author jiuyetx
 * @date 2022/9/13
 */
@Builder
@Data
@AllArgsConstructor
public class MailAccountDto {
    private String host;
    private Integer port;
    private String from;
    private String user;
    private String pass;
    private List<String> receivers;
    private String protocol;
}
