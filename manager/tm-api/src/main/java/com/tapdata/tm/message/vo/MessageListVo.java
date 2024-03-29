package com.tapdata.tm.message.vo;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Date;
import java.util.Map;

@Data
public class MessageListVo {

    private String id;

    private String level;

    private String system;


    private String serverName;
    private String agentName;

    private String agentId;

    private String msg;
    private String title;
    //其实就对应实体类的msg
    private String sourceModule;

    private String sourceId;

    private Boolean read = false;
    private String time;

    @JsonProperty("createTime")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date createAt;

    private String template;
    private Map<String, Object> param;

}
